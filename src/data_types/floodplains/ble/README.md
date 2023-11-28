## Creating the analysis environment.

Pulled latest Docker gdal so we can use the ogrinfo --json flag.

SEE: https://gdal.org/programs/ogrinfo.html

https://github.com/OSGeo/gdal/pkgs/container/gdal/versions

NOTE: alpine-small images do not include full SpatiaLite.
See: https://github.com/OSGeo/gdal/issues/5657

```
docker pull ghcr.io/osgeo/gdal:alpine-normal-3.7.2
alpine-normal-3.7.2: Pulling from osgeo/gdal
9398808236ff: Already exists
4f4fb700ef54: Already exists
c57d439ffa71: Pull complete
fc10941ad418: Pull complete
28246c4ca86d: Pull complete
e6f61df1eb91: Pull complete
43446028cfd9: Pull complete
4fdef73b830b: Pull complete
01e4e1147022: Pull complete
1838b10d640c: Pull complete
a987e7efe06e: Pull complete
fb3dfc04f527: Pull complete
5bc3e32a2c93: Pull complete
aa34ea605f3c: Pull complete
046a9f1e80f2: Pull complete
Digest: sha256:179b6889b97c82fcfd3d1d74cc6254045f9a0bd92107063fe3f1f91a6b6cd257
Status: Downloaded newer image for ghcr.io/osgeo/gdal:alpine-normal-3.7.2
ghcr.io/osgeo/gdal:alpine-normal-3.7.2
```

Installed Node.js in the container.

SEE: https://superuser.com/questions/1125969/how-to-install-npm-in-alpine-linux

```sh
$ docker run --rm -it -v ${PWD}:/data ghcr.io/osgeo/gdal:alpine-normal-3.7.2
/ # apk add --update nodejs npm
fetch https://dl-cdn.alpinelinux.org/alpine/v3.17/main/x86_64/APKINDEX.tar.gz
fetch https://dl-cdn.alpinelinux.org/alpine/v3.17/community/x86_64/APKINDEX.tar.gz
(1/2) Installing nodejs (18.17.1-r0)
(2/2) Installing npm (9.1.2-r0)
Executing busybox-1.35.0-r29.trigger
OK: 288 MiB in 103 packages
```

Note: The package gdal-tools includes ogrmerge.py. See: https://pkgs.alpinelinux.org/contents?branch=edge&name=gdal%2dtools&arch=ppc64le&repo=community

Created an image of the container with the latest GDAL and Node.js versions.

```sh
$ docker container ls
CONTAINER ID   IMAGE                                    COMMAND                  CREATED         STATUS         PORTS                    NAMES
07eefaaa795e   ghcr.io/osgeo/gdal:alpine-normal-3.7.2   "/bin/sh"                4 minutes ago   Up 4 minutes                            youthful_black

$ docker commit 07eefaaa795e avail/gis-analysis
```
