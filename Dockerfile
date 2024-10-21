FROM python:3.11.4-slim
ENV PYTHONUNBUFFERED True
ENV APP_HOME /tag-engine
WORKDIR $APP_HOME
COPY . ./
RUN pip3 install Flask gunicorn
RUN pip3 install --no-cache-dir -r $APP_HOME/requirements/requirements.txt
ENV PORT 8080
CMD exec gunicorn --bind :$PORT --workers 3 --threads 10 --timeout 0 main:app
