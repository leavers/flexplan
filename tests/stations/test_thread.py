from flexplan.workers.base import Worker


class Tester(Worker):
    def echo(self, message: str) -> str:
        return message


def test_thread_station():
    from flexplan.datastructures.future import Future
    from flexplan.datastructures.instancecreator import InstanceCreator
    from flexplan.messages.mail import ContactInfo, Mail, MailMeta
    from flexplan.stations.thread import ThreadStation
    from flexplan.workbench.loop import LoopWorkbench

    station = ThreadStation(
        workbench_creator=InstanceCreator(LoopWorkbench),
        worker_creator=InstanceCreator(Tester),
    )
    station.start()

    future: Future = Future()
    mail = Mail(
        Tester.echo,
        args=("message",),
        meta=MailMeta(
            sender=ContactInfo(),
            receivers=[],
        ),
        future=future,
    )
    station.send(mail)
    station.stop()
    print("wait")
    print(future.result())
