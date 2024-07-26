FROM golang:1.22-alpine as build 
ENV GO111MODULE=on
ENV CGO_ENABLED=0
COPY . /service
WORKDIR /service
RUN go build -o redisbench

FROM alpine:3.17
WORKDIR /service
COPY --from=build /service/redisbench .

CMD [ "./redisbench" ]