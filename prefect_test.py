from prefect import flow, task

@task
def create_message():
    msg = 'hello from task'
    return(msg)

@flow
def something_else():
    result = 10
    return(result)

@flow
def hello_world():
    sub_flow_message = something_else()
    task_message = create_message()
    new_message = task_message + str(sub_flow_message)
    print(task_message)

if __name__ == "__main__":
    hello_world.serve(name="my-test-deployment", cron="* * * * *") # runs every minute