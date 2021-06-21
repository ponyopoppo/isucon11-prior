include_cookbook 'xbuild'

version = '1.16.5'

execute "rm -rf /home/isucon/local/golang; /opt/xbuild/go-install #{version} /home/isucon/local/golang `uname -s | tr [A-Z] [a-z]` `dpkg --print-architecture`" do
  user 'isucon'
  not_if "/home/isucon/local/golang/bin/go version | grep -q 'go#{version} '"
end
