FROM alpine:edge

RUN apk add tzdata

WORKDIR /app

ADD collector /app/


CMD [ "./collector" ]
