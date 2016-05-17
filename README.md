# TeamDynamix Import and Visualization Script

This script connects to your TeamDynamix instance, downloads your data, and then creates a set of Tableau extract files. You should not need to be an administrator to use this script.

## Installation

1. `git clone https://github.com/coopermj/TeamDynamixFun`
1. `cd TeamDynamixFun`
1. Install [Python 2 (2.7.9+)](https://www.python.org/downloads/windows/) (Mac/Linux should have it)
2. Install [Tableau SDK](https://onlinehelp.tableau.com/current/api/sdk/en-us/help.htm#SDK/tableau_sdk_installing.htm%3FTocPath%3D_____3)
3. `pip install -r requirements.txt`
4. `python dailyBuilder.py`
5. Follow the prompts!

You should have a file named TDAnalysis.twbx at the end that you can open with the free [Tableau Reader](http://www.tableau.com/products/reader).

Note that the TeamDynamix API requires using the non-Single Sign On login. You can test your log in at [https://app.teamdynamix.com/TDNext/Login.aspx](https://app.teamdynamix.com/TDNext/Login.aspx) and make sure you can log in. Your username generally is an email address.

