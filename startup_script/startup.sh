#!/bin/sh

export AIRFLOW__CORE__FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
export FERNET_KEY=$AIRFLOW__CORE__FERNET_KEY