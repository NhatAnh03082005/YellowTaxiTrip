FROM mageai/mageai:0.9.76

USER root

COPY mage_project/requirements.txt /tmp/mage-requirements.txt
RUN pip install --no-cache-dir -r /tmp/mage-requirements.txt

WORKDIR /home/src/mage_project
