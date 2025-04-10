# diskbench

A fast, parallel disk I/O benchmarking utility that measures transfer rates by copying temporary files and provides detailed performance metrics.

## Download
The latest release of the agora-uploader can be found [here](https://github.com/GyroTools/diskbench/releases/latest/). Please make sure you choose the correct platform.

## Usage

```
diskbench [global options] TARGET_PATH
```

## Arguments

- **TARGET_PATH**  
  The path to the target disk/directory where files will be copied

## Global Options

- **--source, -p**  
  The source directory to copy files from (defaults to the system temporary directory)

- **--size, -s**  
  Size of temporary files to create (e.g., 1G, 500M) (default: "1G")

- **--workers, -w**  
  Number of parallel copy operations (default: 1)

- **--iterations, -i**  
  Number of times to copy each file (default: 3)

- **--no-cleanup, -n**  
  If specified, temporary files are not deleted after the test

## Examples

Test with a single worker:

```
diskbench --size 500M --iterations 10 /mnt/external_drive
```

Test with multiple parallel workers:

```
diskbench --size 1G --workers 8 --iterations 3 /mnt/external_drive
```