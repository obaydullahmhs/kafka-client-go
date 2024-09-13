FROM golang AS builder
RUN mkdir /kafka-consumer
COPY .. /kafka-consumer
WORKDIR /kafka-consumer
RUN go build .

FROM ubuntu
WORKDIR /kafka-consumer
COPY --from=builder /kafka-consumer/ /kafka-consumer/

ENTRYPOINT ["./consumer"]
