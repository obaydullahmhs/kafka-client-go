FROM golang AS builder
RUN mkdir /kafka-producer
COPY .. /kafka-producer
WORKDIR /kafka-producer
RUN go build .

FROM ubuntu
WORKDIR /kafka-producer
COPY --from=builder /kafka-producer/ /kafka-producer/

ENTRYPOINT ["./producer"]
