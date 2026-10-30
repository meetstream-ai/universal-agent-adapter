FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml uv.lock ./
ARG AGENT_RUNTIME=all
RUN pip install --no-cache-dir 'certifi>=2026.10.19' uv==0.13.5 && \
    case "$AGENT_RUNTIME" in \
      all) uv sync --frozen --no-dev --no-install-project --extra openai --extra langgraph --extra adk --extra crewai ;; \
      openai|langgraph|adk|crewai) uv sync --frozen --no-dev --no-install-project --extra "$AGENT_RUNTIME" ;; \
      *) echo 'Choose all, openai, langgraph, adk or crewai'; exit 1 ;; \
    esac
COPY README.md ./
COPY src ./src
RUN uv pip install --python /app/.venv/bin/python --no-deps -e .
COPY config.toml ./config.toml
RUN sed -i 's|ledger = "actions.sqlite"|ledger = "/data/actions.sqlite"|' config.toml
RUN useradd --uid 10001 --create-home adapter && mkdir /data && chown adapter /data
USER adapter
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8080
CMD ["meetstream-adapter", "serve", "--host", "0.0.0.0", "--port", "8080"]
