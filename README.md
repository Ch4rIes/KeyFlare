# KeyFlare: High-Performance In-Memory Caching Server
Introducing a high-performance in-memory caching server solution, inspired by renowned caching solutions with unique custom implementations tailored for specific use cases.

# Components

## 1. Networking Component
Custom Protocol Parsing: Custom protocol parsing is implemented to cater to the specific requirements of this architecture.
Socket Handling: Built with the capability to handle TCP connections using raw sockets.
Non-blocking IO: The architecture supports non-blocking IO operations through IO multiplexing, ensuring optimal performance in highly concurrent environments.
Timeout for Idle Connections: A timeout feature has been integrated to eliminate idle connections and optimize resource management.

## 2. Custom Data Structure Implementations
HashMap: An efficiently designed structure for rapid key-value pair access.

AVL Tree: Utilized for its self-balancing properties, enhancing performance in various internal program scenarios.
## 3. Key-Value Storage & Serialization
Diverse Data Structure Support: Storage, serialization, and retrieval are enabled for various data structures, including OrderedSet, Int, Double, and String.
Optimized Retrieval: The system ensures rapid data retrieval irrespective of the underlying data structure.

## 4. Multithreaded Design
Asynchronous Large Key Removal: Recognizing the impact of large keys on performance, a multithreaded approach has been adopted to asynchronously remove these burdensome keys.
Performance Boost: This strategy has significantly reduced response times by up to 40%.
Conclusion
This in-memory caching server solution is designed to cater to a wide array of applications with high efficiency. Feedback and contributions are highly encouraged. Dive into the code and collaborate to enhance this project further.





