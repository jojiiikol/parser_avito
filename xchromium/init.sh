
#!/usr/bin/env bash

set -e

echo "[.] Starting tbank container..."

CHROME_PATH=$(find /ms-playwright -name "chrome" -type f | head -1)
echo "[.] Using Chromium: $CHROME_PATH"

echo "[.] Setting up mitmproxy to capture traffic from Chromium and forward it to ${PROXY_GATEWAY_URL}..."
echo "[.] env attrs"
echo "[.] PROXY_GATEWAY_URL: $PROXY_GATEWAY_URL"
echo "[.] PROXY_USERNAME: $PROXY_USERNAME"
echo "[.] PROXY_PASSWORD: $PROXY_PASSWORD"

mitmdump --mode upstream:$PROXY_GATEWAY_URL \
         --upstream-auth $PROXY_USERNAME:$PROXY_PASSWORD \
         -v \
         --listen-host 0.0.0.0 \
         --listen-port 8080 &
MITM_PID=$!
echo "[+] mitmproxy started"



echo "[.] Starting socat to forward traffic *:9222 -> 127.0.0.1:9223"
if netstat -tlpn | grep -q ":9221"; then
    echo "[-] Error: Failed to start socat. Port 9221 is already in use."
    exit 1
fi

socat TCP-LISTEN:9222,fork,bind=0.0.0.0 TCP:127.0.0.1:9223 &
SOCAT_PID=$!
echo "[+] socat started"

echo "[.] Starting Chromium..."
# 127.0.0.1:8080 - mitmproxy proxy wrapper
Xvfb :99 -screen 0 1920x1080x24 &
export DISPLAY=:99

$CHROME_PATH \
    --no-sandbox \
    --ignore-certificate-errors \
    --disable-dev-shm-usage \
    --enable-webgl \
    --enable-gpu-rasterization \
    --use-gl=egl \
    --disable-blink-features=AutomationControlled \
    --disable-automation \
    --disable-infobars \
    --disable-browser-side-navigation \
    --remote-debugging-port=9223 \
    --remote-debugging-address=0.0.0.0 \
    --proxy-server=127.0.0.1:8080 \
    --remote-allow-origins=* \
    --user-data-dir=/tmp/chromium \
    about:blank &
CHROMIUM_PID=$!

echo "[+] Everything is set up and ready!"
wait $CHROMIUM_PID $MITM_PID $SOCAT_PID
