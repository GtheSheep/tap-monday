# tap-monday

`tap-monday` is a Singer tap for Monday.com.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

TODO: Convert streams to incremental

## Installation


```bash
pipx install git+https://github.com/gthesheep/tap-monday.git
```

## Configuration

### Accepted Config Options

* `auth_token` - Authorisation token obtained from following the process in the documentation [here](https://api.developer.monday.com/docs/authentication)
* `board_limit` - Number of boards to fetch at once, default 10

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-monday --about
```

```
{
  "auth_token": "yourauthenticationtoken"
}
```

### Executing the Tap Directly

```bash
tap-monday --version
tap-monday --help
tap-monday --config CONFIG --discover > ./catalog.json
```

## Developer Resources


### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_monday/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-monday` CLI interface directly using `poetry run`:

```bash
poetry run tap-monday --help
```

