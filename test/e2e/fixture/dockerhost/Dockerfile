FROM docker:28-dind
# Alpine's adduser locks new accounts ("!"), and sshd refuses key logins to
# locked accounts, so the password field becomes "*" (no password, not locked).
RUN apk add --no-cache openssh-server sudo \
 && (getent group docker >/dev/null || addgroup -S docker) \
 && adduser -D -s /bin/sh -G docker deploy \
 && sed -i 's/^deploy:!/deploy:*/' /etc/shadow \
 && echo 'deploy ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/deploy && chmod 0440 /etc/sudoers.d/deploy \
 && ssh-keygen -q -t ed25519 -N '' -f /etc/ssh/ssh_host_ed25519_key
RUN printf '%s\n' 'Port 22' 'HostKey /etc/ssh/ssh_host_ed25519_key' \
      'PasswordAuthentication no' 'KbdInteractiveAuthentication no' \
      'PubkeyAuthentication yes' 'PermitRootLogin no' \
      'AuthorizedKeysFile /etc/ssh/deploy_authorized_keys' 'StrictModes no' \
      > /etc/ssh/sshd_config
COPY entrypoint.sh healthcheck.sh /
RUN chmod +x /entrypoint.sh /healthcheck.sh
EXPOSE 22 8080
ENTRYPOINT ["/entrypoint.sh"]
