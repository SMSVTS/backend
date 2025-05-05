from crontab import CronTab

cron = CronTab(user=True)
command = " /venv/bin/activate && python smsvts_flower_market/user_management/user_create.py daily_report"

job = cron.new(command=command, comment="Daily Report Job")

job.setall("30 16 * * *")  

cron.write()
