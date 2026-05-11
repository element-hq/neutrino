FROM rust:1.95-trixie

RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs jq

RUN npm install -g @anthropic-ai/claude-code

RUN git clone https://github.com/Kholtien/claude-connect-matrix-integration /opt/claude-matrix && \
    cd /opt/claude-matrix && npm install

WORKDIR /workspace

# the /matrix:access skill doesn't exist, so just write who is allowed to talk to Claude based on ALLOWED_USERS
CMD ["/bin/sh", "-c", "\
    ACCESS_FILE=\"$HOME/.claude/channels/matrix-e2ee/access.json\" && \
    echo \"Allowing $ALLOWED_USERS\" && \
    mkdir -p \"$(dirname \"$ACCESS_FILE\")\" && \
    printf '%s' \"$ALLOWED_USERS\" | jq '{policy: \"allowlist\", allowFrom: .}' > \"$ACCESS_FILE.tmp\" && \
    chmod 0600 \"$ACCESS_FILE.tmp\" && \
    mv \"$ACCESS_FILE.tmp\" \"$ACCESS_FILE\" && \
    echo ' - To add MCP: claude mcp add matrix -s user -- npx -y tsx /opt/claude-matrix/server.ts' && \
    echo ' - To start claude: claude --dangerously-load-development-channels server:matrix' && \
    exec bash"]