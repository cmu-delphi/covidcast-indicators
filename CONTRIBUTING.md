# How to contribute to this repository

## General workflow for indicators creation and deployment

**tl;dr**

1. Create your new indicator branch from `main`.
2. Build it using the appropriate template, following the guidelines in the included README.md and REVIEW.md files.
3. Make some stuff!
4. When your stuff works, push your `dev-*` branch to remote for review.
5. Consult with a platform engineer for the remaining production setup needs. They will create a branch called `deploy-*` for your indicator.
6. Initiate a pull request against this new branch.
7. Following [the source documentation template](https://github.com/cmu-delphi/delphi-epidata/blob/main/docs/api/covidcast-signals/_source-template.md), create public API documentation for the source. You can submit this as a pull request against the delphi-epidata repository.
8. If your peers like the code, the documentation is ready, and Jenkins approves, deploy your changes by merging the PR.
9. An admin will propagate your successful changes to `main`.
10. Rejoice!

### Starting out

The `main` branch should contain up-to-date code and supporting libraries. This should be your starting point when creating a new indicator.

```shell
# Hint
#
git checkout main
git checkout -b dev-my-feature-branch
```

### Creating your indicator

Create a directory for your new indicator by making a copy of `_template_r` or `_template_python` depending on the programming language you intend to use. The template copies of `README.md` and `REVIEW.md` include the minimum requirements for code structure, documentation, linting, testing, and method of configuration. Beyond that, we don't have any established restrictions on implementation; you can look at other existing indicators see some examples of code layout, organization, and general approach.

- Consult your peers with questions! :handshake:

Once you have something that runs locally and passes tests you set up your remote branch eventual review and production deployment.

```shell
# Hint
#
git push -u origin dev-my-feature-branch
```

You can then set draft public API documentation for people who would fetch this
data from the API. Public API documentation is kept in the delphi-epidata
repository, and there is a [template Markdown
file](https://github.com/cmu-delphi/delphi-epidata/blob/main/docs/api/covidcast-signals/_source-template.md)
that outlines the features that need to be documented. You can create a pull
request to add a new file to `docs/api/covidcast-signals/` for your source. Our
goal is to have public API documentation for the data at the same time as it
becomes available to the public.

### Setting up for review and deployment

Once you have your branch set up you should get in touch with a platform engineer to pair up on the remaining production needs. These include:

- Creating the corresponding `deploy-*` branch in the repo.
- Adding the necessary Jenkins scripts for your indicator.
- Preparing the runtime host with any Automation configuration necessities.
- Reviewing the workflow to make sure it meets the general guidelines and will run as expected on the runtime host.

Once all the last mile configuration is in place you can create a pull request against the correct `deploy-*` branch to initiate the CI/CD pipeline which will build, test, and package your indicator for deployment.

If everything looks ok, you've drafted source documentation, platform engineering has validated the last mile, and the pull request is accepted, you can merge the PR. Deployment will start automatically.

Hopefully it'll be a full on :tada:, after that :crossed_fingers:

If not, circle back and try again.

## Production overview

### Running production code

Currently, the production indicators all live and run on the venerable and perennially useful Delphi primary server (also known generically as "the runtime host").

### Delivering an indicator to the production environment

We use a branch-based git workflow coupled with [Jenkins](https://www.jenkins.io/) and [Ansible](https://www.ansible.com/) to build, test, package, and deploy each indicator individually to the runtime host.

- Jenkins dutifully manages the whole process for us by executing several "stages" in the context of a [CI/CD pipeline](https://dzone.com/articles/learn-how-to-setup-a-cicd-pipeline-from-scratch). Each stage does something unique, building on the previous stage. The stages are:
  - Environment - Sets up some environment-specific needs that the other stages depend on.
  - Build - Create the Python venv on the Jenkins host.
  - Test - Run linting and unit tests.
  - Package - Tar and gzip the built environment.
  - Deploy - Trigger an Ansible playbook to place the built package onto the runtime host, place any necessary production configuration, and adjust the runtime envirnemnt (if necessary).

There are several additional Jenkins-specific files that will need to be created for each indicator, as well as some configuration additions to the runtime host. It will be important to pair with a platform engineer to prepare the necessary production environment needs, test the workflow, validate on production, and ultimately sign off on a production release.
