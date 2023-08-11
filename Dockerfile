FROM alpine:edge

RUN apk add tzdata

COPY dist /app

WORKDIR /app
USER 1001

CMD [ "./main" ]
