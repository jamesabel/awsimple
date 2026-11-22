# Contributing to awsimple

Hi! First of all, thank you for contributing. :heart:

All of the usual sorts of contributions are welcome: bug reports, patches, and feedback.
Feel free to [browse existing issues](https://github.com/jamesabel/awsimple/issues) or [create a new one](https://github.com/jamesabel/awsimple/issues/new).


## Got a problem?

You're welcome to [create an issue](https://github.com/jamesabel/awsimple/issues/new), but please [search existing ones](https://github.com/jamesabel/awsimple/issues) first to see if it's been discussed before.


## Want to submit some code or docs?

Great! 

If you're interested in tackling an [existing issue](https://github.com/jamesabel/awsimple/issues), comment on one to make sure you're on the right track.
If it's an idea you have or a problem not captured in an issue, [create one](https://github.com/jamesabel/awsimple/issues/new) and let's align.


### Dev setup

Requirements:

- [uv](https://docs.astral.sh/uv/), which installs Python (3.12 or higher) and the dependencies

Fork the repo, to make your own personal copy, and then git clone your fork onto your computer.

Once you have that, install project dependencies with:

#### Mac / Linux
```
source make_venv_dev.sh
```

#### Windows
```
make_venv_dev.bat
```

This makes the environment in `.venv`. At this point you should be able to make changes to the codebase and run things,
e.g. the tests with `uv run pytest test_awsimple`.
