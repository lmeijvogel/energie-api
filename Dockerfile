FROM ruby:3.1.2-alpine3.16

RUN apk add --no-cache tini g++ gcc make musl-dev tzdata postgresql-dev

ENV TZ=Europe/Amsterdam
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# -D: don't assign password.
# -h: set homedir
RUN adduser --uid 1000 -D -h /app api

RUN chown -R api /app

WORKDIR /app

COPY Gemfile* /app/

RUN bundle config set --local path '/bundle'
RUN bundle config set --local deployment 'true'
RUN bundle install

USER api
COPY . /app/

ENTRYPOINT ["tini", "--"]
CMD ["bundle", "exec", "ruby", "app.rb", "--port", "4567"]
