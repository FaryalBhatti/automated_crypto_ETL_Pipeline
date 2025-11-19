FROM astrocrpublic.azurecr.io/runtime:3.1-5

USER root

# Install Airflow providers
RUN pip install --no-cache-dir apache-airflow-providers-http apache-airflow-providers-postgres

USER astro
