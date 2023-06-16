# STEDI Human Balance Analytics
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

trains the user to do a STEDI balance exercise;
and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
has a companion mobile app that collects customer data and interacts with the device sensors.
STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

## Introduction
As a data engineer on the STEDI Step Trainer team, you'll need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

Your project work will be evaluated against this rubric. Take a moment to review the requirements of the rubric now, and while you're working on your project to make sure you're meeting all the requirements for deliverables.

## Implementation

### Landing Zone

**Glue Tables:**
- [customer_landing.sql](scripts/customer_landing.sql)
- [accelerometer_landing.sql](scripts/accelerometer_landing.sql)

*Customer Landing*:
![Customer Landing](images/customer_landing.png)
![Customer Landing All](images/customer_landing_2.png)


*Accelerometer Landing*:
![Accelerometer Landing](images/accelerometer_landing.png)


### Trusted Zone
**Glue job scripts**:
- [customer_trusted.sql](scripts/customer_trusted.py)
- [accelerometer_trusted.sql](scripts/accelerometer_trusted.py)
- [customer_trusted_all.sql](scripts/customer_trusted_2.py)


Athena: Trusted Zone Query results:
![Accelerometer Landing](images/customer_trusted.png)


### Curated Zone
**Glue job scripts**:
- [customers_curated.py](scripts/customers_curated.py)
- [step_trainer_trusted.py](scripts/step_trainer_trusted.py)
- [machine_learning_curated.py](scripts/machine_learning_curated.py)

