.. _contributing:

************
Contributing
************

Submitting a pull request
#########################

Opaque SQL enforces linting rules for all contributions. Before making changes and opening a pull request, please do the following (compatible for Ubuntu 18.04):

#. Fork the repository on Github, then clone your fork and set upstream:

   .. code-block:: bash
   
                git clone https://github.com/<your username>/opaque.git
                cd opaque
                git remote add upstream https://github.com/mc2-project/opaque.git

#. Install dependencies:

   .. code-block:: bash
               
                # Coursier and scalafmt
                curl -fLo cs https://git.io/coursier-cli-"$(uname | tr LD ld)"
                chmod +x cs
                ./cs install cs
                echo "" >> ~/.bashrc
                echo "export PATH=$PATH:~/.local/share/coursier/bin" >> ~/.bashrc
                source ~/.bashrc
                rm cs
                cs install scalafmt

                # npm and clang-format
                sudo apt -y update
                sudo apt -y install npm
                sudo npm install -g clang-format

#. Install a Git pre-commit hook:

   .. code-block:: bash
               
                ./${OPAQUE_HOME}/hooks/install-pre-commit

   Alternatively, you can just run the following file right before submitting a PR to create a new commit with linting changes:

   .. code-block:: bash

                ./${OPAQUE_HOME}/scripts/format.sh

Debugging your changes
######################

Opaque SQL supports debugging native C/C++ code with `oegdb <https://github.com/openenclave/openenclave/blob/master/docs/GettingStartedDocs/Debugging.md>`_. There are three steps to do so:

1. Specify the test suite you want to debug in ``${OPAQUE_HOME}/project/resources/run-tests.gdb``.

2. Set a debug environment variable: ``export DEBUG=1``.

3. Run ``${OPAQUE_HOME}/build/sbt oe-gdb``

Note that setting ``DEBUG=1`` builds the enclave code in an insecure state: this **should not** be set for any production system.
