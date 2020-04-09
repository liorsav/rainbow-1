# Rainbow
Apache Rainbow is an end-to-end platform for data engineers & scientists, allowing them to build, train and deploy machine learning models in a robust and agile way. The platform provides the abstractions and declarative capabilities for data extraction & feature engineering followed by model training and serving. Apache Rainbow's goal is to operationalise the machine learning process, allowing data scientists to quickly transition from a successful experiment to an automated pipeline of model training, validation, deployment and inference in production, freeing them from engineering and non-functional tasks, and allowing them to focus on machine learning code and artifacts.

## Rationale
The challenges involved in operationalizing machine learning models are one of the main reasons why many machine learning projects never make it to production.
The process involves automating and orchestrating multiple steps which run on heterogeneous infrastructure - different compute environments, data processing platforms, ML frameworks, notebooks, containers and monitoring tools.
There are no mature standards for this workflow, and most organizations do not have the experience to build it in-house. In the best case, dev-ds-devops teams form in order to accomplish this task together; in many cases, it's the data scientists who try to deal with this themselves without the knowledge or the inclination to become infrastructure experts.
As a result, many projects never make it through the cycle. Those who do suffer from a very long lead time from a successful experiment to an operational, refreshable, deployed and monitored model in production. 
The goal of Apache Rainbow is to simplify the creation and management of machine learning pipelines by data engineers & scientists. The platform provides declarative building blocks which define the workflow, orchestrate the underlying infrastructure,  take care of non functional concerns, enabling focus in business logic / algorithm code.
Some Commercial E2E solutions have started to emerge in the last few years, however, they are limited to specific parts of the workflow, such as Databricks MLFlow. Other solutions are tied to specific environments (e.g. SageMaker on AWS).

## High Level Architecture
Rainbow platform is aimed to provide data engineers & scientists with a solution for end to end flows from model training to real time inference in production. It’s architecture enables and promotes adoption of specific components in existing (non-Rainbow) frameworks, as well as seamless integration with other open source projects. Rainbow was created to enable scalability in ML efforts and after a thorough review of available solutions and frameworks, which did not meet our main KPIs: 
Provide an opinionated but customizable end-to-end workflow
Abstract away the complexity of underlying infrastructure
Support major open source tools and cloud-native infrastructure to carry out many of the steps
Allow teams to leverage their existing investments or bring in their tools of choice into the workflow
We have found that other tech companies in the Israeli Hi-Tech ecosystem also have an interest in such a platform, hence decided to share our work with the community.
The following diagram depicts these main components and where Apache Rainbow comes in:

![raibow-arch1](https://github.com/Natural-Intelligence/rainbow/images/rainbow_001.png)

A classical data scientist workflow includes some base phases: 
_Train, Deploy and Consume._

**The Train phase includes the following tasks:**

1. Fetch -  get the data needed to build a model - usually using SQL
1. Clean - make sure the data is useful for building the model 
1. Prepare - split data and encode features from the data according to model needs 
1. Train - Build the model and tune it
1. Evaluate - make sure the model is correct - run it on a test set, etc…
1. Validate - make sure the model is up to the standards you need

**The Deploy phase includes these tasks:**
1. Deploy - make it available for usage in production
1. Inference - Batch or Real-time - use the model to evaluate data by your offline or online by your applications
1. Consume - The actual use of the models created by applications and ETLs, usually through APIs to the batch or real-time inference that usually rely on Model and Feature stores.
 
Rainbow provides its users a declarative composition capabilities to materialize these steps in a robust way, while exploiting existing frameworks and tools. e.g. Data science frameworks such as scikit-learn, Tensor flow, Keras and such, for running core data science algorithms; as numerous core mechanisms as data stores, processing engines, parallelism, schedulers, code deployment as well as batch and real-time inference.
Rainbow allows the creation and wiring of these kinds of functional and non functional tasks while making the underlying infrastructure used by these tasks very easy to use and even abstracted away entirely. While handling the non-functional aspects as monitoring (in a standard fashion) deployment, scheduling, resource management and execution.

![raibow-arch1](https://github.com/Natural-Intelligence/rainbow/images/rainbow_002.png)

# Basics

## Data pipeline task types
Data science task types - programs based on data scientist code.
Example of tasks that users can create in a data science pipeline:
Data preparation:
SQL ETL - ETLs that can be described via SQL to production based on configuration:
SQL query
Source configuration
Sink configuration
Data transformation
Model training
Model deploy
Batch inference/execution
Update model for real-time inference/execution service
ETL applications - YARN/EMR applications based on user’s code.

Using simple YAML configuration, create your own schedule data pipelines (a sequence of tasks to
perform), application servers,  and more.

## Service types
HTTP server - HTTP server with endpoints mapped to user code. Useful for real time inference/execution of user code by their clients.

## Non-functional
The platform will take care of non-functional aspects of users’ applications, namely:
Build
Deploy
Parallelism (via data partitioning)
Job status metrics
Alerting

## API
Rainbow will introduce a declarative API to define data pipelines and services which references user code by module/path.

## UI
Rainbow’s UI will allow users of rainbow to create pipelines via a web ui (as well as REST API).
For example:
Allow a user to define a pipeline via filling a web form.
Allow a user to define tasks via filling a web form.
For example:
Define an SQL ETL task with dropdowns for available sources/sinks/”tables” in the organization.

## Example YAML config file
```yaml
name: MyPipeline
owner: Bosco Albert Baracus
pipelines:
  - pipeline: my_pipeline
    start_date: 1970-01-01
    timeout_minutes: 45
    schedule: 0 * 1 * *
    metrics:
     namespace: TestNamespace
     backends: [ 'cloudwatch' ]
    tasks:
      - task: my_static_input_task
        type: python
        description: static input task
        image: my_static_input_task_image
        source: helloworld
        env_vars:
          env1: "a"
          env2: "b"
        input_type: static
        input_path: '[ { "foo": "bar" }, { "foo": "baz" } ]'
        output_path: /output.json
        cmd: python -u hello_world.py
      - task: my_parallelized_static_input_task
        type: python
        description: parallelized static input task
        image: my_static_input_task_image
        env_vars:
          env1: "a"
          env2: "b"
        input_type: static
        input_path: '[ { "foo": "bar" }, { "foo": "baz" } ]'
        split_input: True
        executors: 2
        cmd: python -u helloworld.py
      - task: my_task_output_input_task
        type: python
        description: task with input from other task's output
        image: my_task_output_input_task_image
        source: helloworld
        env_vars:
          env1: "a"
          env2: "b"
        input_type: task
        input_path: my_static_input_task
        cmd: python -u hello_world.py
services:
  - service:
    name: my_python_server
    type: python_server
    description: my python server
    image: my_server_image
    source: myserver
    endpoints:
      - endpoint: /myendpoint1
        module: myserver.my_server
        function: myendpoint1func
```

## Example repository structure

[Example repository structure](
https://github.com/Natural-Intelligence/rainbow/tree/master/tests/runners/airflow/rainbow]
)

# Installation

TODO: installation.
