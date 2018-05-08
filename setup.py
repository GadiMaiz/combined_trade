from cx_Freeze import setup, Executable
import sys
import os

base = None

if sys.platform == 'win32':
    base = None


executables = [Executable("flask_orderbook_service.py", base=base)]
os.environ['TCL_LIBRARY'] = r'C:\Program Files\Python36\tcl\tcl8.6'
os.environ['TK_LIBRARY'] = r'C:\Program Files\Python36\tcl\tk8.6'
packages = ["idna", "asyncio"]
options = {
    'build_exe': {

        'packages':packages,
    },

}

setup(
    name = "Bitmain",
    options = options,
    version = "1.0",
    description = 'Combined Trade',
    executables = executables
)