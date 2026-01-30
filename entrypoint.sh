#!/bin/bash

export LOG_LEVEL=${LOG_LEVEL:-"INFO"}
export CELERY_APP=${CELERY_APP:-"nexus_conversations.celery"}
export CELERY_MAX_WORKERS=${CELERY_MAX_WORKERS:-'6'}
export HEALTHCHECK_TIMEOUT=${HEALTHCHECK_TIMEOUT:-"10"}

do_gosu(){
    user="$1"
    shift 1

    is_exec="false"
    if [ "$1" = "exec" ]; then
        is_exec="true"
        shift 1
    fi

    if [ "$(id -u)" = "0" ]; then
        if [ "${is_exec}" = "true" ]; then
            exec gosu "${user}" "$@"
        else
            gosu "${user}" "$@"
            return "$?"
        fi
    else
        if [ "${is_exec}" = "true" ]; then
            exec "$@"
        else
            eval '"$@"'
            return "$?"
        fi
    fi
}

if [[ "start" == "$1" ]]; then
    echo "Starting Conversation MS SQS Consumer"
    export PYTHONPATH="${APP_PATH}:${PYTHONPATH}"
    cd "${APP_PATH}"
    do_gosu "${APP_USER}:${APP_GROUP}" exec python conversation_ms/main.py
elif [[ "web" == "$1" ]]; then
    echo "Starting Gunicorn Web Server"
    do_gosu "${APP_USER}:${APP_GROUP}" exec gunicorn nexus_conversations.wsgi:application \
        --bind 0.0.0.0:8000 \
        --workers ${GUNICORN_WORKERS:-4} \
        --timeout ${GUNICORN_TIMEOUT:-120} \
        --access-logfile - \
        --error-logfile -
elif [[ "celery-worker" == "$1" ]]; then
    celery_queue="celery"
    echo "Starting Celery worker"
    if [ "${2}" ] ; then
        celery_queue="${2}"
    fi
    do_gosu "${APP_USER}:${APP_GROUP}" exec celery \
        -A "${CELERY_APP}" worker \
        -Q "${celery_queue}" \
        -O fair \
        -l "${LOG_LEVEL}" \
        --autoscale=${CELERY_MAX_WORKERS},1
elif [[ "celery-beat" == "$1" ]]; then
    echo "Starting Celery Beat"
    do_gosu "${APP_USER}:${APP_GROUP}" exec celery \
        -A "${CELERY_APP}" beat \
        -l "${LOG_LEVEL}"
elif [[ "healthcheck-celery-worker" == "$1" ]]; then
    celery_queue="celery"
    if [ "${2}" ] ; then
        celery_queue="${2}"
    fi
    HEALTHCHECK_OUT=$(
        do_gosu "${APP_USER}:${APP_GROUP}" celery -A "${CELERY_APP}" \
            inspect ping \
            -d "${celery_queue}@${HOSTNAME}" \
            --timeout "${HEALTHCHECK_TIMEOUT}" 2>&1
    )
    echo "${HEALTHCHECK_OUT}"
    grep -F -qs "${celery_queue}@${HOSTNAME}: OK" <<< "${HEALTHCHECK_OUT}" || exit 1
    exit 0
elif [[ "healthcheck-consumer" == "$1" ]]; then
    # Check if heartbeat file exists and was modified recently (e.g. last 120 seconds)
    # Default path matches main.py default
    heartbeat_file="${2:-/tmp/healthy}"

    if [ ! -f "$heartbeat_file" ]; then
        echo "Heartbeat file not found: $heartbeat_file"
        # Fallback: check if process is running
        if pgrep -f "conversation_ms/main.py" > /dev/null 2>&1; then
            echo "Process is running (fallback)"
            exit 0
        fi
        exit 1
    fi

    # Check modification time
    current_time=$(date +%s)
    file_mod_time=$(stat -c %Y "$heartbeat_file")
    diff=$((current_time - file_mod_time))

    if [ $diff -lt 120 ]; then
        echo "Consumer healthy (heartbeat age: ${diff}s)"
        exit 0
    else
        echo "Consumer unhealthy (heartbeat age: ${diff}s)"
        exit 1
    fi
fi

exec "$@"
