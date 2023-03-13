FROM python:3.10-slim
ENV PYTHONUNBUFFERED True
ENV APP_HOME /tag-engine
WORKDIR $APP_HOME
COPY . ./
RUN pip3 install Flask gunicorn
RUN pip3 install --no-cache-dir -r $APP_HOME/requirements.txt
CMD exec gunicorn --bind :$PORT --workers 2 --threads 8 --timeout 0 main:app
