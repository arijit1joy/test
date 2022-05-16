---------------------------
Command to Run behave
---------------------------

use Command prompt or Pycharm Terminal

inside da_edge_j1939_services\EDGE-J1939-BDD folder

>behave -f pretty

---------------------------
Allure Install
---------------------------

open powershell

a) Install Scoop

> iwr -useb get.scoop.sh | iex

b) install Allure

> scoop install allure

---------------------------
Allure Install allure behave
---------------------------
use Command prompt or Pycharm Terminal

pip install --quiet allure-behave

---------------------------
Run Allure reports
---------------------------
> behave -f allure_behave.formatter:AllureFormatter -o reports
> allure serve reports
