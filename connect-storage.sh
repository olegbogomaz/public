sudo mkdir /mnt/oleg
if [ ! -d "/etc/smbcredentials" ]; then
sudo mkdir /etc/smbcredentials
fi
if [ ! -f "/etc/smbcredentials/rgfuncdevcanc001b734.cred" ]; then
    sudo bash -c 'echo "username=rgfuncdevcanc001b734" >> /etc/smbcredentials/rgfuncdevcanc001b734.cred'
    sudo bash -c 'echo "password=SO6+gx8mqaN1KBEx1ai3+aweDxESte8ri/8d3MqoC6Zek4mR1Yx2f9sy7vfWLH9taqwxBhb5IE6n+AStiewHsg==" >> /etc/smbcredentials/rgfuncdevcanc001b734.cred'
fi
sudo chmod 600 /etc/smbcredentials/rgfuncdevcanc001b734.cred

sudo bash -c 'echo "//rgfuncdevcanc001b734.file.core.windows.net/oleg /mnt/oleg cifs nofail,credentials=/etc/smbcredentials/rgfuncdevcanc001b734.cred,dir_mode=0777,file_mode=0777,serverino,nosharesock,actimeo=30" >> /etc/fstab'
sudo mount -t cifs //rgfuncdevcanc001b734.file.core.windows.net/oleg /mnt/oleg -o credentials=/etc/smbcredentials/rgfuncdevcanc001b734.cred,dir_mode=0777,file_mode=0777,serverino,nosharesock,actimeo=30
