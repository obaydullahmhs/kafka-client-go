FROM golang AS builder
RUN mkdir /kafka-consumer-group-sync
COPY .. /kafka-consumer-group-sync
WORKDIR /kafka-consumer-group-sync
RUN go build .

FROM ubuntu
WORKDIR /kafka-consumer-group-sync
COPY --from=builder /kafka-consumer-group-sync/ /kafka-consumer-group-sync/

ENTRYPOINT ["./consumer-group-sync"]
