FROM continuumio/miniconda3:latest

WORKDIR /tmp/HPC-Simulator
COPY environment.yml .
COPY api/ /tmp/HPC-Simulator/api/
COPY framework/ /tmp/HPC-Simulator/framework/
RUN conda env create -f environment.yml

WORKDIR /tmp/HPC-Simulator/framework/dashboard
EXPOSE 8050
ENTRYPOINT ["conda", "run", "-n", "hpc-sim", "python", "dashboard.py"]
