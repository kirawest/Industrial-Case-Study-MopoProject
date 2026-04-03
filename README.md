# This repo is currently under construction

Once constructed, this repo should be an easy way to reproduce the Industrial Case workflow, both for collaboration and for others to use in the future. It can also serve as an example for others that want to build additional data on top of the European Case.

    !!! note These instructions are not correct. This is a draft, copied from a different repo.

# First Time Set Up Instructions

## Install Python??

## Project Setup

1. Clone this repository
1. Download the data zipped folder from Zenodo > Right click > Extract All > Choose the Raw-Data folder

1. EITHER open the project in VSCode and open a powershell terminal 

    OR open a regular powershell terminal and cd into the project folder: `cd [path to folder]`

<!-- 1. Create & activate a new python environment:
    ```
    py -3.13 -m venv .venv
    .\venv\Scripts\Activate.ps1
    ``` -->

1. Install python dependencies: 

    `python -m pip install -r requirements.txt`

<!-- 1. Install julia dependencies:

    `julia --project=. -e "using Pkg; Pkg.instantiate()"` -->

1. Run spinetoolbox: `spinetoolbox`

1. Open the project: *File > Open Project > Industrial-Case-Study-MopoProject*

<!-- 1. Double-click on each Julia tool and set the Project to the project folder 

    (This makes sure it sees the correct julia environment and packages) -->

1. Double-click on each intermediate datastore (pink icons) > *New SpineDB > Okay* 
    
    (This will create sqlite files in the default folders SpineToolbox chooses.)

TODO: Set to Consumer

TODO: Make sure tools use python env created above

You should be good to go!

# Updating the Workflow (collaborating)

Once you've completed the first-time setup, this is how you can start-up when returning to work on the project.

1. Open a terminal in the project folder, or open it VSCode.

1. Get any updates from others: 

    `git fetch origin --prune`

1. Merge the changes with your working directory: 

    `git merge --ff-only origin/main`

    (ff-only is a safety measure so it breaks if the changes conflict with your local changes)

Now you can open spinetoolbox and work on things. If you only work in the data and *running* the pipeline, just save and close. If you make changes to the pipeline that you want to share, follow these steps:

1. Check what has changed: 

    `git status`

1. If you want to see changes in a specific file:

     `git diff [FILE]`

1. **ADD** whichever changes you want to share:

     `git add [FILE]`

1. Check everything is correct: 

    `git status`

1. OPTIONAL: Undo any changes you don't want to share:

    `git restore [FILE]`

1. **COMMIT** your changes: 

    `git commit -m "My insightful message"`

1. **PUSH** your changes to your own remote fork:

    `git push remote-name branch-name`

1. Click on the link, or go to your remote online to create a **PULL REQUEST** to the shared repo.


# TODO Running the Workflow

- Launch spinetoolbox
- Tooling order
- Avoiding rerunning from raw
- Scenario filters
- Config files
