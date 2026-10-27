## Airflow + dbt Core + DuckDB in Docker and WSL: Setup, Troubleshooting, and GitHub Deployment

Project: airflow-project

Environment: Windows 11 → WSL Ubuntu → VS Code → Docker Desktop

Scope: Two dbt projects (sales_dbt and britmart_analytics), Apache Airflow orchestration, portable Python verification, and GitHub publication.

Reconstruction note: This guide documents the sequence and fixes recorded during the project. Some early terminal error messages were not preserved verbatim; those are described as symptoms, not fabricated quotations. Paths and commands reflect the working setup. Review your current docker-compose.yml, Dockerfile.airflow-dbt, and DAGs before reusing commands in a new environment.

#### 1. What we built

The objective was to run dbt Core transformations against DuckDB under Apache Airflow, from a reproducible Docker environment, while keeping the code editable in VS Code connected to WSL Ubuntu.

We developed two projects:

- sales_dbt — A small CSV sales dataset; dbt model computes estimated tax and total with tax; custom data-quality test; Airflow DAG executes dbt; a Python script reads the resulting DuckDB table.
- britmart_analytics — An orders.csv seed, an stg_orders.sql transformation, dbt configuration, and its Airflow DAG.
Windows 11 / VS Code
       |
       v
WSL Ubuntu: ~/airflow-project/
       |               \
       |                +-- .venv/ (optional LOCAL Python only)
       v
Docker Desktop / Docker Compose
       |
       v
Airflow container (Python 3.13, Airflow 3.3.2, dbt Core, dbt-duckdb)
       |
       +-- Airflow DAGs -> dbt models/tests
       |                      |
       |                      v
       +-- ./airflow bind mount -> DuckDB database (*.duckdb)
                                |
                                v
                      Local read-only verifier

#### Repository structure

airflow-project/
├── .dockerignore
├── .env                 # PRIVATE: never commit
├── .env.example         # Safe placeholders for configuration
├── .gitignore
├── Dockerfile.airflow-dbt
├── docker-compose.yml
├── README.md
├── assignments/
│   └── airflow_dbt_duckdb/
│       └── verify_duckdb.py
└── airflow/
    ├── dags/
    │   ├── sales_dbt_pipeline.py
    │   └── airflow_dbt_pipeline.py
    └── dbt/
        ├── sales_dbt/
        │   ├── dbt_project.yml
        │   ├── profiles.yml
        │   ├── models/example_model.sql
        │   ├── seeds/sales.csv
        │   └── tests/assert_positive_total.sql
        └── britmart_analytics/
            ├── dbt_project.yml
            ├── profiles.yml
            ├── models/stg_orders.sql
            └── seeds/orders.csv

*.duckdb, .venv/, dbt-generated artifacts, logs, and secrets are intentionally excluded from Git.

#### 2. Fix the development environment:
VS Code, WSL, and Docker

2.1 Use the right VS Code execution environment

The first essential correction was ensuring VS Code was attached to WSL Ubuntu, rather than using a Windows terminal for Linux paths and commands.

1. Open VS Code in its WSL mode (Remote/WSL connection).
2. Open the project directory from Ubuntu: ~/airflow-project.
3. Check the integrated terminal is an Ubuntu shell and uses a prompt such as namrota_ghosh@...:~/airflow-project$.
4. Confirm the tools are accessible:
pwd
whoami
python3 --version
docker --version
docker compose version

Why:
Windows and Ubuntu have different paths, Python installations, SSH keys, and virtual environments. Commands executed in Windows PowerShell are not automatically equivalent to those executed in WSL.

2.2 Start Docker Desktop and enable WSL integration

Initially Docker Desktop was not running. Start it in Windows, ensure WSL integration is enabled for the Ubuntu distribution, then test in WSL:
- docker info
- docker ps
An earlier Docker socket permission issue in the broader setup was solved using Docker group access. For a system where group membership is not configured:
sudo usermod -aG docker "$USER"

# Exit/restart the WSL shell for the new group membership to apply.
groups
Do not run arbitrary chmod 777 /var/run/docker.sock: weakening the socket permissions is not the recommended remedy.
2.3 Understand the bind mount
The Compose application used this important mapping:
volumes:
  - ./airflow:/opt/airflow
Therefore the same files have two apparent paths:
| File | WSL path | Path inside container |
|---|---|---|
| DuckDB database | `~/airflow-project/airflow/dbt/sales_dbt/sales.duckdb` | `/opt/airflow/dbt/sales_dbt/sales.duckdb` |
| dbt project | `~/airflow-project/airflow/dbt/sales_dbt` | `/opt/airflow/dbt/sales_dbt` |
| Airflow DAG | `~/airflow-project/airflow/dags/sales_dbt_pipeline.py` | `/opt/airflow/dags/sales_dbt_pipeline.py` |

This distinction later explained why hardcoded Docker paths failed for locally executed Python scripts.

#### 3. Choose compatible Airflow, Python, and dbt versions

3.1 What caused concern

We needed dbt to run inside the same Airflow image. Installing arbitrary latest versions could introduce Python compatibility or Airflow dependency conflicts. We tested and settled on a known-working combination:
| Component | Validated version |
|---|---|
| Base Docker image | `apache/airflow:3.3.2-python3.13` |
| Python inside Docker | `3.13.15` |
| Apache Airflow | `3.3.2` |
| dbt Core | `1.11.16` |
| dbt DuckDB adapter | `1.11.1` |

The local WSL Python was later verified as Python 3.14; that is a separate installation, not the container interpreter. The local standalone verifier ultimately used DuckDB 2.0.0 and pandas 3.1.0. Do not assume Docker uses these same DuckDB/pandas versions; check it directly.

3.2 Dockerfile

The tested Dockerfile followed this structure:

FROM apache/airflow:3.3.2-python3.13

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    "dbt-core==1.11.16" \
    "dbt-duckdb==1.11.1" \
    pandas sqlalchemy pymysql \
    && pip check


pip check helps detect broken installed Python dependency requirements. Pinning the major runtime packages makes the environment more predictable.

3.3 Test the image before changing the live service

We built a temporary test image (my-airflow:dbt-test), then a GitHub-oriented check image (my-airflow:github-test). The latter was checked for Python/Airflow/dbt versions and pip check success.

Reusable validation pattern:

docker build -f Dockerfile.airflow-dbt -t my-airflow:dbt-test .

docker run --rm --entrypoint python my-airflow:dbt-test \
  -c "import sys, airflow, duckdb; print(sys.version); print(airflow.__version__); print(duckdb.__version__)"

docker run --rm --entrypoint dbt my-airflow:dbt-test --version

Note: Verify the actual DuckDB version printed by Docker rather than assuming it matches your WSL installation.

3.4 Wire the Dockerfile into Compose

The Compose service sleek-airflow was subsequently configured to build from this Dockerfile and use image tag my-airflow:dbt:

services:
  sleek-airflow:
    build:
      context: .
      dockerfile: Dockerfile.airflow-dbt
    image: my-airflow:dbt
    user: "${AIRFLOW_UID:-50000}:0"
    volumes:
      - ./airflow:/opt/airflow
    ports:
      - "8080:8080"
      - "8081:8081"

This is a focused excerpt, not a replacement for the complete existing Compose file (which also had environment variables and other service configuration). After editing:

docker compose config --quiet
docker compose up -d --build

config --quiet succeeded during final review. The project historically ran on the tested image tag my-airflow:dbt-test; my-airflow:dbt is the intended reproducible Compose image tag.

#### 4. Resolve container filesystem permissions and dbt runtime errors

These were some of the most instructive problems in the project.

4.1 Problem: dbt executable inaccessible under the overridden user

At one point, dbt was installed in the image but could not be executed with the configured runtime UID. The discovered issue involved access to the installed executable under /home/airflow/.local/bin/dbt when the container was running with a different user identity.

Diagnosis: Installation success is not the same as runtime access. A Docker image can contain a package while its user, PATH, or home-directory permissions prevent it from running.

Fix used: Align the running container with host file ownership using UID 1000, GID 0:
user: "${AIRFLOW_UID:-50000}:0"
With AIRFLOW_UID=1000 in .env, this resolves to 1000:0. We validated the configuration using Docker Compose and then successfully ran dbt from the Airflow service. This avoided making the bind-mounted project world-writable.

Useful diagnostics:
id
id -u
docker compose config --quiet
docker compose exec sleek-airflow id
docker compose exec sleek-airflow which dbt

4.2 Problem: dbt cannot write logs/ or target/

A second permission problem arose because dbt normally attempts to create/write logs and target under the project directory. The ./airflow bind mount and runtime UID caused ownership/write-access friction.

Fix: Redirect dbt-generated output to writable /tmp directories inside the container:
docker compose exec \
  -e DBT_LOG_PATH=/tmp/sales-dbt-logs \
  -e DBT_TARGET_PATH=/tmp/sales-dbt-target \
  -e DBT_PROFILES_DIR=/opt/airflow/dbt/sales_dbt \
  sleek-airflow \
  dbt debug --project-dir /opt/airflow/dbt/sales_dbt

The three environment variables have distinct jobs:
| Variable | Purpose |
|---|---|
| `DBT_LOG_PATH` | Writable location for dbt logs |
| `DBT_TARGET_PATH` | Writable location for compiled SQL, manifests, run results |
| `DBT_PROFILES_DIR` | Where dbt looks for `profiles.yml` |


Important: /tmp/sales-dbt-logs and /tmp/sales-dbt-target refer to container paths here, not host paths.

4.3 Problem: dbt works in Docker but not from WSL

Running dbt directly in Ubuntu originally failed because dbt was installed in the container image, not in the Ubuntu Python installation.

The working approach was to run it through Compose:

docker compose exec -e DBT_LOG_PATH=/tmp/sales-dbt-logs \
  -e DBT_TARGET_PATH=/tmp/sales-dbt-target \
  -e DBT_PROFILES_DIR=/opt/airflow/dbt/sales_dbt \
  sleek-airflow dbt --version

The principle is to run dependencies in the environment where they are installed, rather than assuming all terminals share one Python environment.

#### 5. Set up the Sales dbt project

5.1 dbt_project.yml and profiles.yml

The project lives at airflow/dbt/sales_dbt/. Its dbt project references profile sales_dbt, and its models were configured to materialize as tables.

The DuckDB profile points to:

/opt/airflow/dbt/sales_dbt/sales.duckdb
This is correct for dbt inside the container. Because the parent directory is bind-mounted, the resulting database file also exists under the WSL project folder.

5.2 Input CSV

airflow/dbt/sales_dbt/seeds/sales.csv:
order_id,customer,amount
1,Alice,500
2,Bob,700
3,Charlie,300
4,Diana,900

5.3 Transformation model

airflow/dbt/sales_dbt/models/example_model.sql reads the CSV directly and computes two columns. The recorded approach used DuckDB's read_csv_auto:

SELECT
    order_id,
    customer,
    amount,
    amount * 0.18 AS estimated_tax,
    amount * 1.18 AS total_with_tax
FROM read_csv_auto(
    '/opt/airflow/dbt/sales_dbt/seeds/sales.csv',
    header = true
)

The SQL above expresses the recorded transformation logic; keep your committed SQL as the source of truth if formatting or explicit types differ.
Because this model reads the CSV directly, a dbt seed task is not required for the Sales model.

5.4 Custom data-quality test

airflow/dbt/sales_dbt/tests/assert_positive_total.sql:

SELECT *
FROM {{ ref('example_model') }}
WHERE total_with_tax <= 0

A dbt singular test passes when the SQL query returns zero rows. This test fails if any transformed order has a non-positive total.

5.5 Run the model and test

docker compose exec \
  -e DBT_LOG_PATH=/tmp/sales-dbt-logs \
  -e DBT_TARGET_PATH=/tmp/sales-dbt-target \
  -e DBT_PROFILES_DIR=/opt/airflow/dbt/sales_dbt \
  sleek-airflow \
  dbt build --project-dir /opt/airflow/dbt/sales_dbt --select example_model

dbt build executes the selected model and eligible related tests. A separate dbt test --select example_model also passed 1/1 in our verification.

#### 6. Integrate dbt with Apache Airflow

6.1 Sales DAG

File: airflow/dags/sales_dbt_pipeline.py
DAG ID: sales_dbt_pipeline

The DAG defined two sequential BashOperator tasks:
check_dbt  -->  build_models_and_test
(dbt debug)    (dbt build --select example_model)
Both tasks set the dbt profile directory and writable log/target paths. The DAG also used append_env=True so the subprocess retained the container's existing environment.

A conceptual task command is:
dbt build --project-dir /opt/airflow/dbt/sales_dbt --select example_model
The DAG imports correctly and was triggered from the Airflow UI at http://localhost:8080. Both tasks completed successfully.

6.2 Debugging DAG discovery and execution

When a DAG is absent from the UI or fails:

1. Confirm the file is under the bind-mounted airflow/dags/ directory.
2. Inspect container status and logs:
   docker compose ps
   docker compose logs --tail=100 sleek-airflow
3. Check Airflow DAG import errors using the CLI supported by the installed Airflow version.
4. Read the specific task log in the Airflow UI to separate Python imports, dbt errors, file paths, and permissions.
5. Confirm DBT_PROFILES_DIR, DBT_LOG_PATH, DBT_TARGET_PATH are set for the DAG task (not only for a manual shell command).
The crucial distinction is that manual dbt success and Airflow task success are separate checks. We verified both.

6.3 BritMart project

File: airflow/dbt/britmart_analytics/
DAG file: airflow/dags/airflow_dbt_pipeline.py
DAG ID: britmart_airflow_dbt_pipeline

The BritMart project contains seeds/orders.csv and models/stg_orders.sql; its workflow performs dbt connection/model operations against its own DuckDB database. The seed must be loaded before a staging model that uses ref('orders') can succeed. Consult the committed DAG for the exact tasks currently automated; do not assume seed loading is in that DAG.

6.4 dbt documentation

The project also served dbt documentation using the additional port 8081. A typical documentation generation step is:
docker compose exec \
  -e DBT_LOG_PATH=/tmp/sales-dbt-logs \
  -e DBT_TARGET_PATH=/tmp/sales-dbt-target \
  -e DBT_PROFILES_DIR=/opt/airflow/dbt/sales_dbt \
  sleek-airflow \
  dbt docs generate --project-dir /opt/airflow/dbt/sales_dbt
Serving docs requires a process bound to an appropriate interface and port. In the completed sessions, the dbt docs UI was successfully accessed on http://localhost:8081 for BritMart. Do not assume generating docs for Sales automatically makes its docs available at the same port.

#### 7. Verify the transformed DuckDB table with Python

7.1 Original Docker-specific script
The original assignments/airflow_dbt_duckdb/verify_duckdb.py connected to:
duckdb.connect(
    '/opt/airflow/dbt/sales_dbt/sales.duckdb',
    read_only=True
)
That path works inside the container, but not directly when Python is executed in WSL because /opt/airflow is a container mount path.

We initially tested the script using the image and read-only bind mounts:
docker run --rm --user "1000:0" --entrypoint python \
  -v "$(pwd)/airflow:/opt/airflow:ro" \
  -v "$(pwd)/assignments:/opt/assignments:ro" \
  my-airflow:dbt-test \
  /opt/assignments/airflow_dbt_duckdb/verify_duckdb.py
It printed four records and the expected values.

7.2 Make the script portable with pathlib
When the user wanted to click Run Python File in VS Code or run it directly in WSL, we removed the hardcoded container path:

from pathlib import Path
import duckdb

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DB_PATH = PROJECT_ROOT / 'airflow' / 'dbt' / 'sales_dbt' / 'sales.duckdb'


def verify_sales_table():
    if not DB_PATH.exists():
        raise FileNotFoundError(f'Database not found: {DB_PATH}')

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        query = '''
        SELECT order_id, customer, amount, estimated_tax, total_with_tax
        FROM main.example_model
        ORDER BY order_id
        '''
        df = con.execute(query).fetchdf()
        print('Sales transformation results:')
        print(df.to_string(index=False))
        print(f'\nTotal records: {len(df)}')
    finally:
        con.close()


if __name__ == '__main__':
    verify_sales_table()

Why it works:
 __file__ locates this script; .parent.parent ascends from assignments/airflow_dbt_duckdb/ to the repository root; the / operator assembles the path for the current machine. It remains dependent on the repository's relative folder structure and the database already existing. This exact version is portable across local clones, but does not automatically work with the earlier Docker command, since that command mounts assignments under /opt/assignments rather than beneath the same repository root as airflow. For Docker verification, either retain the Docker-specific variant or mount the entire project at one common parent path and adjust permissions.

7.3 Debug the initial pathlib mistakes

Two coding mistakes were corrected:

Missing import:
from pathlib import Path
Incorrect use of a literal string:
#### WRONG: literally contains the letters PROJECT_ROOT
DB_PATH = 'PROJECT_ROOT/airflow/dbt/sales_dbt/sales.duckdb'

#### RIGHT: joins the variable's value with path components
DB_PATH = PROJECT_ROOT / 'airflow' / 'dbt' / 'sales_dbt' / 'sales.duckdb'

#### 8. Fix WSL Python package errors without disturbing Docker

After switching to direct local execution, the initial errors were:
ModuleNotFoundError: No module named 'duckdb'
/usr/bin/python3: No module named pip
bash: .venv/bin/activate: No such file or directory
Command 'python' not found

Why:

WSL Ubuntu initially had system python3, but not local DuckDB/pandas or pip; no .venv had been created; Ubuntu's default executable was python3, not python.

8.1 Install supporting packages
sudo apt update
sudo apt install -y python3-pip python3-venv
python3 -m pip --version
Verified result:
pip 25.1.1 ... (python 3.14)
8.2 Create an isolated local virtual environment
cd ~/airflow-project
python3 -m venv .venv
source .venv/bin/activate
python -m pip install duckdb pandas
Verified installations:
Successfully installed duckdb-2.0.0 ... pandas-3.1.0 ...
DuckDB: 2.0.0
pandas: 3.1.0
Once .venv is active, python refers to the environment's interpreter, and it can import DuckDB and pandas.

8.3 Run the script successfully

python assignments/airflow_dbt_duckdb/verify_duckdb.py
Observed output:
Sales transformation results:
 order_id customer  amount  estimated_tax  total_with_tax
        1    Alice     500           90.0           590.0
        2      Bob     700          126.0           826.0
        3  Charlie     300           54.0           354.0
        4    Diana     900          162.0          1062.0

Total records: 4

Git ignore status was also verified:

git check-ignore -v .venv/


##### Isolation:

pip install in .venv does not alter the Docker image.

But both environments can see the same .duckdb file via the bind mount; avoid simultaneous writes, and consider database-format compatibility if DuckDB versions differ.

#### 9. Version control and GitHub publication

9.1 Make the repository reproducible and safe

The committed materials included:
- .gitignore to exclude .env, local databases, .venv/, logs, dbt generated files, caches, generated credentials and backups.
- .dockerignore to keep unnecessary local files out of build context.
- .env.example with placeholders, rather than real MySQL and Gmail credentials.
- Dockerfile.airflow-dbt and docker-compose.yml to document/build the runtime.
- Both dbt projects, both Airflow DAGs, the verifier, and root README.
We intentionally did not stage unrelated QuickCart inputs, rejected data, previous workflows, or private artifacts as part of the initial assignment commit.

9.2 Stage only the intended assignment files

The initial selection was staged explicitly; BritMart was added later:
git add .gitignore .dockerignore .env.example README.md \
  Dockerfile.airflow-dbt docker-compose.yml \
  airflow/dags/sales_dbt_pipeline.py airflow/dbt/sales_dbt/ \
  assignments/airflow_dbt_duckdb/verify_duckdb.py

git add airflow/dbt/britmart_analytics/
git add airflow/dags/airflow_dbt_pipeline.py
A generated dbt sample model directory was removed from the Sales staging index, leaving the actual sales model. The BritMart project's default generated dbt example models remained staged in that initial commit.

9.3 Pre-commit checks

git diff --cached --name-only
git diff --cached --check
git diff --cached --name-only | grep -Ei \
  '(\.env$|\.duckdb|\.db$|\.log$|\.backup$|\.user\.yml)'
The whitespace check initially identified trailing whitespace in docker-compose.yml; it was corrected and re-staged. The final exclusion-pattern check returned no matches. Filename checks are not a complete secret scan: always review staged content for hardcoded tokens/passwords, too.

9.4 Commit identity error

Attempting the first commit initially failed:
Author identity unknown
fatal: empty ident name ... not allowed
Cause: Git in WSL did not have user.name and user.email configured, even if another Git installation had been configured elsewhere.

Fix:
git config --global user.name "Your Git author name"
git config --global user.email "your-github-email@example.com"
The user then committed successfully:
9716b09 (HEAD -> main) Add Airflow dbt DuckDB pipelines for Sales and BritMart

9.5 GitHub SSH authentication error

The first push failed with:
git@github.com: Permission denied (publickey).
fatal: Could not read from remote repository.
The first SSH attempt had added GitHub to known_hosts, which records the server's host key; it does not authenticate the local user.

Investigations showed:
~/.ssh/ contained only known_hosts
ssh-add -l -> Could not open a connection to your authentication agent

Resolution:
1. Generate a new SSH keypair within WSL Ubuntu:
   ssh-keygen -t ed25519 -C "your-github-email@example.com"
2. Start the agent in the current terminal, which exposes SSH_AUTH_SOCK:
   eval "$(ssh-agent -s)"
   echo "$SSH_AUTH_SOCK"
3. Load the private key and verify the agent knows about it:
   ssh-add ~/.ssh/id_ed25519
   ssh-add -l
4. Register the public key from ~/.ssh/id_ed25519.pub under GitHub Settings → SSH and GPG keys → New SSH key. Never upload or paste the private key (id_ed25519).
5. Confirm the GitHub identity:
   ssh -T git@github.com
6. Push the existing commit (no new commit necessary):
   git push -u origin main
The user confirmed that both projects and associated files became visible in the GitHub repository Namrota/dbt_docker_airflow_setup.

9.6 Push the later pathlib improvement

The portable verifier was updated after the first GitHub push. To publish this later change:
git status
git add assignments/airflow_dbt_duckdb/verify_duckdb.py
git commit -m "Make DuckDB verification script portable"
git push
Check the current Git status to verify whether this follow-up commit has already been pushed; the conversation confirmed the earlier repository push but not a later push of the portable verifier.

#### 10. Error → root cause → fix reference

Symptom / error	Root cause	Fix or diagnostic
Docker commands unavailable in WSL	Docker Desktop stopped or WSL integration inactive	Start Docker Desktop; verify WSL integration and docker ps
Docker socket permission denied	Ubuntu user lacked effective socket access	Docker group membership; restart shell; avoid unsafe socket chmod
VS Code commands run in unexpected environment	VS Code terminal attached to Windows, not WSL	Connect VS Code to WSL Ubuntu and reopen project folder
dbt package present but executable inaccessible	Runtime UID/path permissions mismatched image installation	Align service user with host (AIRFLOW_UID=1000, 1000:0), verify runtime executable
dbt unable to create log/target directories	Bind mount write restrictions	Set DBT_LOG_PATH and DBT_TARGET_PATH to /tmp/...
dbt cannot locate profiles.yml	Profile search path incorrect	Set DBT_PROFILES_DIR to dbt project path
dbt: command not found from Ubuntu	dbt was installed in Docker only	Use docker compose exec ... sleek-airflow dbt ...
Airflow DAG not visible	DAG import error or wrong mount path	Check mapped airflow/dags, CLI import errors, logs
Python verifier fails to open /opt/airflow/... locally	Container-only absolute path	Build a WSL-local path dynamically with pathlib
NameError: Path is not defined	Missing from pathlib import Path	Import Path
Database path contains literal PROJECT_ROOT	Quoted variable name as string	Use PROJECT_ROOT / 'airflow' / ...
No module named duckdb	WSL Python lacked dependency	Create .venv; install DuckDB/pandas there
No module named pip	Ubuntu pip not installed	sudo apt install python3-pip python3-venv
.venv/bin/activate missing	Virtual environment never created	python3 -m venv .venv
python: command not found	System Ubuntu exposes python3	Use python3 outside venv; python inside activated venv
fatal: empty ident name	Git identity unconfigured in WSL	Set git config --global user.name/user.email
Permission denied (publickey)	WSL SSH key not registered or available	Generate key, start agent, ssh-add, register .pub on GitHub
Could not open a connection to your authentication agent	SSH agent not running/connected in that terminal	eval "$(ssh-agent -s)"; check SSH_AUTH_SOCK


11. How to reproduce from a fresh clone

These are the broad steps for someone cloning the repository; the existing README/Compose file should be consulted for environment variables and local prerequisites.
##### 1. Clone via SSH (after registering an SSH authentication key).
git clone git@github.com:Namrota/dbt_docker_airflow_setup.git
cd dbt_docker_airflow_setup

##### 2. Create local configuration; edit placeholder values as appropriate.
cp .env.example .env

##### 3. Validate and build the Docker application.
docker compose config --quiet
docker compose up -d --build

##### 4. Run the Sales project inside Docker.
docker compose exec \
  -e DBT_LOG_PATH=/tmp/sales-dbt-logs \
  -e DBT_TARGET_PATH=/tmp/sales-dbt-target \
  -e DBT_PROFILES_DIR=/opt/airflow/dbt/sales_dbt \
  sleek-airflow dbt build \
  --project-dir /opt/airflow/dbt/sales_dbt \
  --select example_model

##### 5. Open Airflow UI at http://localhost:8080
#####    - Trigger sales_dbt_pipeline and inspect both task logs.

##### 6. Optional local read-only DuckDB verification.

    python3 -m venv .venv
    source .venv/bin/activate
    python -m pip install duckdb pandas
    python assignments/airflow_dbt_duckdb/verify_duckdb.py
    Caveats: The database is generated locally and is not checked into Git. It must be built before verification. The local DuckDB library may differ from the image's version; use read-only verification when the database is not being written. For BritMart, load any needed seeds before executing models that reference them.

### 12. Lessons learned:

1. WSL, Windows, Docker, and Python venv are distinct environments. Always check where a command is running and which interpreter/user it uses.
2. Image-build success is not runtime success. Check effective UID, executables, mount permissions, and task logs.
3. Bind mounts make files visible on both sides but do not erase path differences. Container paths such as /opt/airflow are not host paths.
4. dbt generated output directories need writable paths. Explicit DBT_LOG_PATH and DBT_TARGET_PATH solved the observed permission problems.
5. Use pathlib for portable local paths and explicitly account for Docker mount layout if the same script must run in both environments.
6. Test each layer separately: image package versions → dbt CLI → dbt model/test → Airflow DAG tasks → output database → Git upload.
7. Keep secrets and generated files out of Git, and validate staged changes before committing.
8. Git commit identity and SSH authentication are separate. user.name/user.email signs metadata; an SSH key authenticates a GitHub push.
13. Final validated outcomes
- [x] VS Code working in WSL Ubuntu; Docker Desktop and Compose accessible.
- [x] Python 3.13.15 + Apache Airflow 3.3.2 + dbt Core 1.11.16 + dbt-duckdb 1.11.1 verified in  a test image.
- [x] dbt permission and writable-path issues resolved.
- [x] Sales dbt model produced four expected sales rows.
- [x] Custom Sales dbt singular test passed (1/1).
- [x] sales_dbt_pipeline discovered and both Airflow tasks completed successfully.
- [x] BritMart dbt project and DAG integrated; BritMart docs UI served on port 8081.
- [x] Portable verify_duckdb.py successfully ran from WSL Python virtual environment.
- [x] Local .venv/ excluded by .gitignore.
- [x] Initial commit 9716b09 created and pushed to GitHub, containing Sales and BritMart.
- [ ] Confirm portable pathlib verifier update has also been committed and pushed (done locally after the original push).