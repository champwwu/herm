FROM herm:latest AS base

WORKDIR /app

# Copy mock exchange server binary
COPY --from=base /app/mock_exchange_server /app/mock_exchange_server
COPY --from=base /app/config /app/config

EXPOSE 8080

CMD ["./mock_exchange_server"]
