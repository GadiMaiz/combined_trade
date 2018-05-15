vc_redist.x86.exe /install /quiet
nssm install "Bitmain Trade Service" bitmain_trade_service.exe "-p 5000"
net start "Bitmain Trade Service"
copy "Orders Tracker.url" "%systemdrive%:\Users\All Users\Desktop"