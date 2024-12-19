# Installation

## Source

```
git clone https://github.com/satriobio/bulkvis.git
cd bulkvis
git checkout siren
conda env create -f env.yaml
```

## Docker

First build the image from the dockerfile.

```
docker build -t pileupy:latest .
```

Run tool from the image

```
docker run -it -v $(pwd):/data/ pileupy:latest
```


