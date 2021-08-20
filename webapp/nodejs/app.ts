import createFastify, { FastifyReply, FastifyRequest } from 'fastify';
import fastifyCookie from 'fastify-cookie';
import fastifyFormbody from 'fastify-formbody';
import fastifyMysql from 'fastify-mysql';
import path from 'path';
import fs from 'fs';
import { ulid } from 'ulid';

type MySQLResultRows = Array<any> & { insertId: number };
type MySQLColumnCatalogs = Array<any>;
interface MySQLQueryable {
    query(
        sql: string,
        params?: ReadonlyArray<any>
    ): Promise<[MySQLResultRows, MySQLColumnCatalogs]>;
}

interface MySQLClient extends MySQLQueryable {
    beginTransaction(): Promise<void>;
    commit(): Promise<void>;
    rollback(): Promise<void>;
    release(): void;
}

declare module 'fastify' {
    interface FastifyInstance {
        mysql: MySQLQueryable & {
            getConnection(): Promise<MySQLClient>;
        };
    }
}

interface User {
    id: string;
    email: string;
    nickname: string;
    staff: boolean;
    created_at: Date;
}

interface Schedule {
    id: string;
    title: string;
    capacity: number;
    reserved: number;
    reservations: Reservation[];
    created_at: Date;
}

interface Reservation {
    id: string;
    schedule_id: string;
    user_id: string;
    user: User;
    created_at: Date;
}

async function getCurrentUser(request: FastifyRequest) {
    const uidCookie = request.cookies.user_id;
    if (!uidCookie) {
        return null;
    }
    const conn = await fastify.mysql.getConnection();
    try {
        const [[user]] = await conn.query(
            'SELECT * FROM `users` WHERE `id` = ? LIMIT 1',
            [uidCookie]
        );
        return user || null;
    } catch (e) {
        return null;
    } finally {
        conn.release();
    }
}

function sendJSON(reply: FastifyReply, data: any, statusCode: number) {
    reply
        .header('Content-Type', 'application/json; charset=utf-8')
        .status(statusCode)
        .send(data);
}

function sendErrorJSON(reply: FastifyReply, error: Error, statusCode: number) {
    fastify.log.error(error);
    reply
        .header('Content-Type', 'application/json; charset=utf-8')
        .status(statusCode)
        .send({ error });
}

async function requiredLogin(request: FastifyRequest, reply: FastifyReply) {
    if (await getCurrentUser(request)) {
        return true;
    }

    sendErrorJSON(reply, new Error('login required'), 401);
    return false;
}

async function requiredStaffLogin(
    request: FastifyRequest,
    reply: FastifyReply
) {
    if (
        (await getCurrentUser(request)) &&
        (await getCurrentUser(request)).staff
    ) {
        return true;
    }
    sendErrorJSON(reply, new Error('login required'), 401);
    return false;
}

async function getReservations(request: FastifyRequest, s: Schedule) {
    const conn = await fastify.mysql.getConnection();
    try {
        const [rows] = await conn.query(
            'SELECT * FROM `reservations` WHERE `schedule_id` = ?',
            [s.id]
        );
        let reserved = 0;
        s.reservations = [];
        for (const reservation of rows) {
            reservation.user = await getUser(request, reservation.user_id);
            s.reservations.push(reservation);
            reserved++;
        }
        s.reserved = reserved;
    } catch (e) {
        throw e;
    } finally {
        conn.release();
    }
}

async function getReservationsCount(request: FastifyRequest, s: Schedule) {
    const conn = await fastify.mysql.getConnection();
    try {
        const [rows] = await conn.query(
            'SELECT * FROM `reservations` WHERE `schedule_id` = ?',
            [s.id]
        );
        let reserved = 0;
        for (const _reservation of rows) {
            reserved++;
        }
        s.reserved = reserved;
    } catch (e) {
        throw e;
    } finally {
        conn.release();
    }
}

async function getUser(request: FastifyRequest, id: string) {
    const conn = await fastify.mysql.getConnection();
    try {
        const [[user]] = await conn.query(
            'SELECT * FROM `users` WHERE `id` = ? LIMIT 1',
            [id]
        );
        if (
            (await getCurrentUser(request)) &&
            !(await getCurrentUser(request)).staff
        ) {
            user.email = '';
        }
        return user;
    } catch (e) {
        return null;
    } finally {
        conn.release();
    }
}

async function generateID(conn: MySQLClient, table: string) {
    let id = ulid(Date.now());
    while (true) {
        try {
            let [rows] = await conn.query(
                `SELECT 1 FROM \`${table}\` WHERE \`id\` = ? LIMIT 1`,
                [id]
            );
            if (!rows.length) {
                break;
            }
        } catch (e) {
            id = ulid(Date.now());
            continue;
        }
    }
    return id;
}

const fastify = createFastify({ logger: true });

fastify.register(fastifyCookie, {
    secret: 'my-secret',
});
fastify.register(fastifyMysql, {
    host: process.env.DB_HOST ?? '127.0.0.1',
    port: parseInt(process.env.DB_PORT ?? '3306'),
    user: process.env.DB_USER ?? 'isucon',
    password: process.env.DB_PASS ?? 'isucon',
    database: process.env.DB_NAME ?? 'isucon2021_prior',
    promise: true,
});
fastify.register(fastifyFormbody);

fastify.post('/initialize', async function initializeHandler(request, reply) {
    const conn = await fastify.mysql.getConnection();
    try {
        await conn.beginTransaction();

        await conn.query('TRUNCATE `reservations`');
        await conn.query('TRUNCATE `schedules`');
        await conn.query('TRUNCATE `users`');
        const id = await generateID(conn, 'users');
        await conn.query(
            'INSERT INTO `users` (`id`, `email`, `nickname`, `staff`, `created_at`) VALUES (?, ?, ?, true, NOW(6))',
            [id, 'isucon2021_prior@isucon.net', 'isucon']
        );

        await conn.commit();
        sendJSON(reply, { Language: 'nodejs' }, 200);
    } catch (e) {
        await conn.rollback();
        sendErrorJSON(reply, e, 500);
    } finally {
        conn.release();
    }
});

fastify.get('/api/session', async function sessionHandler(request, reply) {
    sendJSON(reply, await getCurrentUser(request), 200);
});

fastify.post('/api/signup', async function signupHandler(request, reply) {
    const conn = await fastify.mysql.getConnection();
    try {
        await conn.beginTransaction();
        const { email, nickname } = request.body as any;
        const id = await generateID(conn, 'users');
        await conn.query(
            'INSERT INTO `users` (`id`, `email`, `nickname`, `created_at`) VALUES (?, ?, ?, NOW(6))',
            [id, email, nickname]
        );
        const [[{ created_at }]] = await conn.query(
            'SELECT `created_at` FROM `users` WHERE `id` = ? LIMIT 1',
            [id]
        );
        const user: User = {
            id,
            email,
            nickname,
            staff: false,
            created_at,
        };
        await conn.commit();
        sendJSON(reply, user, 200);
    } catch (e) {
        await conn.rollback();
        sendErrorJSON(reply, e, 500);
    } finally {
        conn.release();
    }
});

fastify.post('/api/login', async function loginHandler(request, reply) {
    const { email } = request.body as any;
    const conn = await fastify.mysql.getConnection();
    try {
        const [[user]] = await conn.query(
            'SELECT * FROM `users` WHERE `email` = ? LIMIT 1',
            [email]
        );
        reply.setCookie('user_id', user.id, {
            path: '/',
            maxAge: 86400,
            httpOnly: true,
        });
        sendJSON(reply, user, 200);
    } catch (e) {
        sendErrorJSON(reply, e, 403);
    } finally {
        conn.release();
    }
});

fastify.post(
    '/api/schedules',
    async function createScheduleHandler(request, reply) {
        if (!(await requiredStaffLogin(request, reply))) {
            return;
        }
        const conn = await fastify.mysql.getConnection();
        try {
            await conn.beginTransaction();
            let id = await generateID(conn, 'schedules');
            let { title, capacity: capacityStr } = request.body as any;
            const capacity = parseInt(capacityStr);

            await conn.query(
                'INSERT INTO `schedules` (`id`, `title`, `capacity`, `created_at`) VALUES (?, ?, ?, NOW(6))',
                [id, title, capacity]
            );
            const [[{ created_at }]] = await conn.query(
                'SELECT `created_at` FROM `schedules` WHERE `id` = ?',
                [id]
            );
            await conn.commit();
            const schedule = {
                id,
                title,
                capacity,
                created_at,
            };
            sendJSON(reply, schedule, 200);
        } catch (e) {
            await conn.rollback();
            sendErrorJSON(reply, e, 500);
        } finally {
            conn.release();
        }
    }
);

fastify.post(
    '/api/reservations',
    async function createReservationHandler(request, reply) {
        if (!(await requiredLogin(request, reply))) {
            return;
        }
        const conn = await fastify.mysql.getConnection();
        try {
            await conn.beginTransaction();
            const id = await generateID(conn, 'reservations');
            const { scheduleId } = request.body as any;
            const userId = (await getCurrentUser(request)).id;
            let rows: any[] = [];
            [rows] = await conn.query(
                'SELECT 1 FROM `schedules` WHERE `id` = ? LIMIT 1 FOR UPDATE',
                [scheduleId]
            );
            if (rows.length !== 1) {
                await conn.rollback();
                return sendErrorJSON(
                    reply,
                    new Error('schedule not found'),
                    403
                );
            }

            [rows] = await conn.query(
                'SELECT 1 FROM `reservations` WHERE `schedule_id` = ? AND `user_id` = ? LIMIT 1',
                [scheduleId, userId]
            );
            if (rows.length !== 1) {
                return sendErrorJSON(reply, new Error('already taken'), 403);
            }

            const [[{ capacity }]] = await conn.query(
                'SELECT `capacity` FROM `schedules` WHERE `id` = ? LIMIT 1',
                [scheduleId]
            );

            [rows] = await conn.query(
                'SELECT * FROM `reservations` WHERE `schedule_id` = ?',
                [scheduleId]
            );

            let reserved = 0;
            for (const _row of rows) {
                reserved++;
            }

            if (reserved >= capacity) {
                await conn.rollback();
                return sendErrorJSON(
                    reply,
                    new Error('capacity is already full'),
                    403
                );
            }

            await conn.query(
                'INSERT INTO `reservations` (`id`, `schedule_id`, `user_id`, `created_at`) VALUES (?, ?, ?, NOW(6))',
                [id, scheduleId, userId]
            );

            const [[{ created_at }]] = await conn.query(
                'SELECT `created_at` FROM `reservations` WHERE `id` = ?',
                [id]
            );

            await conn.commit();
            const reservation = {
                id,
                schedule_id: scheduleId,
                user_id: userId,
                created_at,
            };
            sendJSON(reply, reservation, 200);
        } catch (e) {
            await conn.rollback();
            sendErrorJSON(reply, e, 500);
        } finally {
            conn.release();
        }
    }
);

fastify.get('/api/schedules', async function schedulesHandler(request, reply) {
    const conn = await fastify.mysql.getConnection();
    try {
        const [rows] = await conn.query(
            'SELECT * FROM `schedules` ORDER BY `id` DESC'
        );
        const schedules: Schedule[] = [];
        for (const schedule of rows) {
            await getReservationsCount(request, schedule);
            schedules.push(schedule);
        }

        sendJSON(reply, schedules, 200);
    } catch (e) {
        sendErrorJSON(reply, e, 500);
    } finally {
        conn.release();
    }
});

fastify.get<{ Params: { id: string } }>(
    '/api/schedules/:id',
    async function scheduleHandler(request, reply) {
        const { id } = request.params;
        const conn = await fastify.mysql.getConnection();
        try {
            const [[schedule]] = await conn.query(
                'SELECT * FROM `schedules` WHERE `id` = ? LIMIT 1',
                [id]
            );

            await getReservations(request, schedule);

            sendJSON(reply, schedule, 200);
        } catch (e) {
            sendErrorJSON(reply, e, 500);
        } finally {
            conn.release();
        }
    }
);

fastify.get('*', async function htmlHandler(request, reply) {
    let realpath = path.join(__dirname, 'public', request.url);
    try {
        const stat = await fs.promises.stat(realpath);
        if (!stat.isDirectory()) {
            const file = fs.createReadStream(realpath);
            reply
                .header(
                    'Content-Type',
                    realpath.endsWith('.js')
                        ? 'application/javascript'
                        : realpath.endsWith('.css')
                        ? 'text/css'
                        : undefined
                )
                .send(file);
            return;
        }
    } catch (e) {}
    realpath = path.join(__dirname, 'public', 'index.html');

    try {
        const file = fs.createReadStream(realpath);
        reply
            .header('Content-Type', 'text/html; chartset=utf-8')
            .status(200)
            .send(file);
    } catch (e) {
        sendErrorJSON(reply, e, 500);
    }
});

fastify.listen(process.env.PORT || 9292, (err, address) => {
    if (err) {
        fastify.log.error('error', err);
        return;
    }
    fastify.log.info(`listening on ${address}`);
});
