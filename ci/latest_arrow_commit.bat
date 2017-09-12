@echo off
git clone --depth=1 --quiet \
    git://github.com/apache/arrow "%USERPROFILE%\\arrow"
git --git-dir="%USERPROFILE%\\arrow\\.git" rev-parse HEAD
