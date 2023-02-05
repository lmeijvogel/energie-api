FROM ruby:3.2.0-alpine3.17

RUN apk add --no-cache influxdb

ENV TZ=Europe/Amsterdam
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY Gemfile Gemfile.lock /app/
WORKDIR /app

RUN mkdir /bundler
RUN bundle config set deployment 'true'
RUN bundle config set path '/bundler'
RUN bundle config set without 'development'
RUN bundle install

CMD bundle exec ruby import_new_data.rb
