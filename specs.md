SPECIFICATIONS
Here's a producer-consumer exercise involving file writes and network sockets. This will help you practice concurrent programming, file I/O, queueing, and network communication.  This is a simple simulation of a media upload service.

There are two main components

1) Producer: Reads the a video file, uploads it to the media upload service (consumer).
2) Consumer:  Accepts simultaneous media uploads.  Saves the uploaded videos to a single folder. Displays and previews the 10 seconds of the uploaded video file.
3) gRPC: Communication component between the produce and consumer.

Both the producer and consumer runs on a diffrent virtual machine and communicate with each other via the network / sockets.



INPUT
The program accepts inputs from the user.

p - Number of producer threads / instances.  Each thread must read from a separate folder containing the videos.

c - Number of consumer threads

q - max queue lenght.  For simplicity we will use a leaky bucket design, additional videos beyond the queue capacity will be dropped.



OUTPUT
The output of the program should show the following:

1) The consumer has a GUI (can be window or browser based)
2) The GUI will show the uploaded videos, preview the 10 seconds of the video on mouse hover.
3) Display the video on click.



BONUS
1) The consumer program can tell the producer processes that the queue is full.
2) Duplicate detection.
3) Video compression.



DELIVERABLES
1) Source code
2) Video Demonstration (test cases to be provided later)
3) Source code + build/compilation steps
4) Slides containing the following:

Key implementation steps
Queueing details
Breakdown of producer and consumer concepts applied
Synchronization mechanisms used to solve the problem
Implementation and justification of the use of gRPC