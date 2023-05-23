# Kafka_TSD_2023

Tutorial repository for Apache Kafka TSD 2023.

Slides
https://www.canva.com/design/DAFjBI2_CUc/Sa2GGoAssJOqM8jCrE6oTg/edit?utm_content=DAFjBI2_CUc&utm_campaign=designshare&utm_medium=link2&utm_source=sharebutton

Tasks:
1. Set up provided Kafka Docker instance and try sample code. Your task is to change the topic and sent Data Structure. Let it be list of your classes and marks. The consumer must calculate your GPA.
2. Change your code so the data is no longer hard coded. Right now producer should be able to write it and send ad-hoc. It's something like telnet. Also add second consumer and producer. Everything should work in one group and topic - right know change the partitioning in topic to 5. 
3. In this task all you need to start is one producer that creates infinite number of messages to one topic with partitioning and 5 consumers assigned to 3 groups (2, 2 and 1 in each). Message "finished" from producers forces consumers to stop receiving. Each group should communicate (via another topic, acting as a producer)  and calculate a mean. The mean must be sent back to producer (another topic!). At the end producer should display all means. 
