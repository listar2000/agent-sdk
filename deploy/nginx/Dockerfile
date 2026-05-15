# Production nginx LB for agent-sdk on Railway.
#
# Drop this in as its own Railway service in front of an N-replica
# agent-sdk service. The nginx config templates Railway env vars at
# container start (via the official nginx image's envsubst entrypoint).
#
# Required env vars (set in the Railway dashboard for this service):
#
#   AGENT_SDK_UPSTREAM   space- or comma-separated "<replica_id>=<host>:<port>"
#                        pairs. Each pair maps the value of the agent_sdk_route
#                        cookie (= the originating replica's id) to its
#                        private hostname inside Railway's network.
#
#   Example:
#     AGENT_SDK_UPSTREAM="r0=agent-sdk-r0.railway.internal:7778 \
#                         r1=agent-sdk-r1.railway.internal:7778 \
#                         r2=agent-sdk-r2.railway.internal:7778 \
#                         r3=agent-sdk-r3.railway.internal:7778"
#
#   AGENT_SDK_FALLBACK   single host:port used when the sticky cookie is
#                        missing or names an unknown replica. Typically the
#                        service-wide RR DNS — Railway will round-robin.
#                        Example: agent-sdk.railway.internal:7778
#
#   PORT                 set by Railway automatically (the public port). nginx
#                        listens here. Default 7778 if you're running locally.
#
# A small entrypoint script renders the upstream / map blocks from
# AGENT_SDK_UPSTREAM into nginx config before nginx starts.

FROM nginx:1.27-alpine

# Drop the default vhost (we ship our own).
RUN rm -f /etc/nginx/conf.d/default.conf

# nginx.conf and the entrypoint that templates upstream blocks at startup.
COPY nginx.conf /etc/nginx/nginx.conf
COPY entrypoint.sh /docker-entrypoint.d/20-template-upstreams.sh
RUN chmod +x /docker-entrypoint.d/20-template-upstreams.sh

# Railway picks the public port via $PORT.
EXPOSE 7778
