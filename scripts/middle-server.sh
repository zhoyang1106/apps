if [ -z "$1" ]; then
    echo "Usage: $0 <python_script>"
    exit 1
fi

source /home/pi/apps/manager_server/.venv/bin/activate
python3 "$1"


