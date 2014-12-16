# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

$install = <<INSTALL
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv F7B8CEA6056E8E56
echo "deb http://www.rabbitmq.com/debian/ testing main" > /etc/apt/sources.list.d/rabbitmq.list
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv D208507CA14F4FCA
echo "deb http://packages.erlang-solutions.com/debian precise contrib" > /etc/apt/sources.list.d/erlang-solutions.list

apt-get -q update
apt-get -y -q install python-pip rabbitmq-server python3 python3-pip
apt-get -q clean
echo "[{rabbit, [{loopback_users, []}]}]." > /etc/rabbitmq/rabbitmq.config
service rabbitmq-server restart
rabbitmq-plugins enable rabbitmq_management

pip install pamqp
pip3 install pamqp
INSTALL

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "Ubuntu"
  config.vm.provision "shell", inline: $install
  config.vm.synced_folder ".", "/home/vagrant/src"
  config.berkshelf.enabled = false
  if Vagrant.has_plugin?("vagrant-vbguest") then
    config.vbguest.auto_update = false
  end
  config.vm.network :forwarded_port, host: 5672, guest: 5672
  config.vm.network :forwarded_port, host: 15672, guest: 15672
end
