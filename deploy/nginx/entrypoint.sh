#!/bin/sh
# Render upstream blocks into /etc/nginx/nginx.conf from Railway env vars.
#
# Inputs:
#   AGENT_SDK_UPSTREAM   space- or comma-separated "<id>=<host>:<port>" pairs.
#                        Example: "r0=agent-sdk-r0.railway.internal:7778
#                                  r1=agent-sdk-r1.railway.internal:7778"
#   PORT                 listen port (Railway sets this; default 7778).
#
# Writes:
#   /etc/nginx/nginx.conf with the template markers substituted.
#
# Runs from /docker-entrypoint.d/ before nginx starts.

set -eu

CONF=/etc/nginx/nginx.conf
LISTEN_PORT="${PORT:-7778}"
UPSTREAM_RAW="${AGENT_SDK_UPSTREAM:-}"

if [ -z "${UPSTREAM_RAW}" ]; then
    echo "entrypoint: AGENT_SDK_UPSTREAM is required (space/comma-separated id=host:port pairs)" >&2
    exit 1
fi

# Normalise: commas -> spaces, collapse whitespace.
UPSTREAM_NORMALISED=$(echo "${UPSTREAM_RAW}" | tr ',' ' ' | tr -s '[:space:]' ' ')

STICKY_MAP=""
SERVERS=""
for PAIR in ${UPSTREAM_NORMALISED}; do
    case "${PAIR}" in
        *=*)
            ID="${PAIR%%=*}"
            HOST="${PAIR#*=}"
            ;;
        *)
            echo "entrypoint: bad upstream entry '${PAIR}' (expected id=host:port)" >&2
            exit 1
            ;;
    esac
    STICKY_MAP="${STICKY_MAP}        \"${ID}\"    ${HOST};\n"
    SERVERS="${SERVERS}        server ${HOST};\n"
done

# Substitute the three template markers. Use sed; the templates contain
# the exact ``# {{NAME}}`` strings so the match is unambiguous.
TMP=$(mktemp)
awk -v sticky="${STICKY_MAP}" -v servers="${SERVERS}" -v port="${LISTEN_PORT}" '
    /# {{STICKY_MAP}}/            { gsub(/# {{STICKY_MAP}}/, sticky); }
    /# {{FALLBACK_HASH_SERVERS}}/ { gsub(/# {{FALLBACK_HASH_SERVERS}}/, servers); }
    /# {{LISTEN_PORT}}/           { gsub(/# {{LISTEN_PORT}}/, port); }
    { print }
' "${CONF}" > "${TMP}"

# awk gsub on a string with embedded \n preserves them literally — convert
# back to real newlines so nginx parses correctly.
sed -i 's/\\n/\n/g' "${TMP}"

mv "${TMP}" "${CONF}"

echo "entrypoint: rendered ${CONF} with $(echo "${UPSTREAM_NORMALISED}" | wc -w) upstreams on :${LISTEN_PORT}"
