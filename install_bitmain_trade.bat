vc_redist.x86.exe /install /quiet
net stop "Bitmain Trade Service"
nssm remove "Bitmain Trade Service" confirm
nssm install "Bitmain Trade Service" bitmain_trade_service.exe "-p 5000 -e Bitstamp/Kraken -l none"
nssm set "Bitmain Trade Service" AppDirectory %CD%
net start "Bitmain Trade Service"
copy "Orders Tracker.url" "%systemdrive%\Users\All Users\Desktop"