# Deploy `trade_dashborad` on Google Cloud Mumbai

This guide assumes:

- your GitHub repo is `https://github.com/vknowledge-123/trade_dashborad.git`
- your domain is `ionequant.live`
- your DNS is managed by Cloudflare
- you want to deploy on a Google Cloud VM in Mumbai (`asia-south1`)

## 1. Create the Google Cloud project

1. Sign in to Google Cloud Console.
2. Create a new project.
3. Attach billing to the project.
4. In the top search bar, search for `Compute Engine`.
5. Click `Enable`.

## 2. Create a VM in Mumbai

Recommended beginner VM:

- Region: `asia-south1` (Mumbai)
- Zone: `asia-south1-b`
- Machine: `e2-small` to start
- Boot disk: `Ubuntu 24.04 LTS`
- Disk size: `20 GB`

Allow:

- `Allow HTTP traffic`
- `Allow HTTPS traffic`

## 3. Reserve a static IP

Reserve a static external IP and attach it to the VM. This keeps your domain pointed at the same IP even after restarts.

## 4. SSH into the VM

Use the `SSH` button in Compute Engine to open a terminal.

## 5. Install Docker and Docker Compose

Run:

```bash
sudo apt update
sudo apt install -y ca-certificates curl gnupg nginx certbot python3-certbot-nginx git
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo \"$VERSION_CODENAME\") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker $USER
newgrp docker
```

Check installation:

```bash
docker --version
docker compose version
nginx -v
```

## 6. Clone your repo

Run:

```bash
cd ~
git clone https://github.com/vknowledge-123/trade_dashborad.git
cd trade_dashborad
```

## 7. Create the production env file

Copy the template:

```bash
cp .env.production.example .env.production
nano .env.production
```

Set at least:

- `SESSION_SECRET_KEY`
- `PASSWORD_PEPPER`
- `SESSION_HTTPS_ONLY=1`

To generate strong secrets:

```bash
python3 - <<'PY'
import secrets
print("SESSION_SECRET_KEY=" + secrets.token_urlsafe(48))
print("PASSWORD_PEPPER=" + secrets.token_urlsafe(48))
PY
```

Paste those values into `.env.production`.

## 8. Start the app with Docker

Build and start:

```bash
docker compose -f docker-compose.prod.yml up -d --build
```

Check containers:

```bash
docker compose -f docker-compose.prod.yml ps
```

View logs:

```bash
docker compose -f docker-compose.prod.yml logs -f app
```

At this point the app should be running on `127.0.0.1:8000` inside the VM.

## 9. Point the domain from Cloudflare to the VM

In Cloudflare DNS:

1. Create an `A` record for `@` pointing to your VM static IP.
2. Create an `A` record for `www` pointing to the same IP.
3. For the first certificate setup, keep both records as `DNS only`.

## 10. Configure Nginx reverse proxy

Copy the included config:

```bash
sudo cp deploy/nginx/ionequant.live.conf /etc/nginx/sites-available/ionequant.live
sudo ln -s /etc/nginx/sites-available/ionequant.live /etc/nginx/sites-enabled/ionequant.live
sudo nginx -t
sudo systemctl reload nginx
```

If the default site causes confusion, remove it:

```bash
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl reload nginx
```

Now test in the browser:

- `http://ionequant.live`

## 11. Add HTTPS with Let's Encrypt

Run:

```bash
sudo certbot --nginx -d ionequant.live -d www.ionequant.live
```

Choose the option to redirect HTTP to HTTPS when Certbot asks.

Test renewal:

```bash
sudo certbot renew --dry-run
```

## 12. Turn Cloudflare proxy back on

After HTTPS works on the origin:

1. In Cloudflare DNS, switch the records from `DNS only` to `Proxied` if you want Cloudflare protection.
2. In Cloudflare SSL/TLS mode, use `Full (strict)`.

## 13. Open the app

Visit:

- `https://ionequant.live`
- `https://ionequant.live/admin`

If no admin exists yet, create it from `/admin/setup`.

## 14. Useful Docker commands

Restart app:

```bash
docker compose -f docker-compose.prod.yml restart app
```

Rebuild after code changes:

```bash
cd ~/trade_dashborad
git pull
docker compose -f docker-compose.prod.yml up -d --build
```

Stop everything:

```bash
docker compose -f docker-compose.prod.yml down
```

See logs:

```bash
docker compose -f docker-compose.prod.yml logs -f
```

## 15. Where your data is stored

- SQLite database: Docker volume `app_data`
- Redis data: Docker volume `redis_data`

This means your app data survives container restarts.

## 16. Recommended production settings

Before using the app publicly:

- keep `SESSION_HTTPS_ONLY=1`
- use strong secrets for `SESSION_SECRET_KEY` and `PASSWORD_PEPPER`
- set `ADMIN_IP_ALLOWLIST` to your public IP if possible
- finish admin 2FA setup
- keep the VM updated

Update packages later with:

```bash
sudo apt update && sudo apt upgrade -y
```

## 17. Common beginner issues

### Domain opens Cloudflare error page

- Check that the Cloudflare `A` record points to the VM static IP.
- Confirm the VM firewall allows ports `80` and `443`.
- Confirm Nginx is running:

```bash
sudo systemctl status nginx
```

### Domain opens but app is unavailable

Check Docker logs:

```bash
docker compose -f docker-compose.prod.yml logs -f app
```

### Certbot fails

- Keep Cloudflare as `DNS only` during the first certificate issue.
- Wait for DNS propagation.
- Make sure `http://ionequant.live` already reaches your VM before running Certbot.

### Admin login is blocked

- Check `ADMIN_IP_ALLOWLIST` in `.env.production`.
- Remove it temporarily if your public IP changed.
