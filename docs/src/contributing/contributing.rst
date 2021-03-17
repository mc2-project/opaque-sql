.. _contributing:

************
Contributing
************

Opaque uses a pre-commit hook to ensure that all commits are formatted to the desired style. Before making changes and opening a pull request, please do the following (compatible for Ubuntu 18.04):

#. Fork the repository on Github, then clone your fork and set upstream:

   .. code-block:: bash
   
                git clone https://github.com/<your username>/opaque.git
                cd opaque
                git remote add upstream https://github.com/mc2-project/opaque.git

#. Install hook dependencies:

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

#. Install hook dependencies:

   .. code-block:: bash
               
                .hooks/install-pre-commit