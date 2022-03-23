#!/usr/bin/env bash

if [[ $# == 0 ]]; then
  exec python
else
  USER_COMMAND=$1
  USER_OPTION="${@:2}"
  if [[ ${USER_COMMAND} == "apiserver" ]]; then
    exec uvicorn app.apiserver.main:app ${USER_OPTION}
  elif [[ ${USER_COMMAND} == "scheduler" ]]; then
    exec uvicorn app.scheduler.main:app ${USER_OPTION}
  elif [[ ${USER_COMMAND} == "worker" ]]; then
    exec python app/worker/main.py ${USER_OPTIONS}
  elif [[ ${USER_COMMAND} == "jupyter" ]]; then
    exec jupyter notebook --config /opt/codepack/config/jupyter_notebook_config.py ${USER_OPTION}
  else
    exec ${USER_COMMAND} ${USER_OPTION}
  fi
fi
