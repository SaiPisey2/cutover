FROM alpine:3.22
# Two users: cutover (passwordless sudo) and nosudo. Alpine's adduser
# locks new accounts ("!"), and sshd refuses key logins to locked
# accounts, so the password field becomes "*" (no password, not locked).
RUN apk add --no-cache openssh-server sudo coreutils \
 && for u in cutover nosudo; do \
      adduser -D -s /bin/sh "$u" \
      && sed -i "s/^$u:!/$u:*/" /etc/shadow \
      && mkdir -p "/home/$u/.ssh" && chmod 700 "/home/$u/.ssh" && chown -R "$u:$u" "/home/$u/.ssh"; \
    done \
 && echo 'cutover ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/cutover && chmod 0440 /etc/sudoers.d/cutover \
 && ssh-keygen -q -t ed25519 -N '' -f /etc/ssh/ssh_host_ed25519_key
COPY sshd_config /etc/ssh/sshd_config
EXPOSE 22
CMD ["/usr/sbin/sshd", "-D", "-e"]
