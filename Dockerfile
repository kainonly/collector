FROM alpine:edge

RUN apk add tzdata

COPY dist /app
WORKDIR /app

CMD [ "./main" ]