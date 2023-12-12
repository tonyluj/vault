Vault
--------
Vault is an easy-to-use, secure storage for personal data.

The reason for developing this project is because I have a lot of private data that needs to be stored, such as photos, music, and videos. I hope that the data can be saved for a long time without too much cost, but the current solution cannot meet my needs. :
1. Many commercial solutions are not open source. I cannot confirm how my data is stored, which makes it impossible for me to restore it. At the same time, the black box system also causes data security risks;
2. The cost and security of the simple and crude redundancy mechanism, especially the local disk RAID solution, do not meet my needs. Sometimes cloud storage is needed to improve reliability;
3. Lack of development and expansion capabilities, such as the need for tiered storage to support more cloud storage updates.

Recent milestones for this project are:
1. Supports tiered storage, which means I can use different storage solutions based on the frequency of data access to reduce costs;
2. Support disaster recovery mechanism and provide a more flexible data backup mechanism, such as backup on local disk, or AWS, Alibaba Cloud, etc.;
3. It has a simple and easy-to-use interface without too much magic;
4. Sufficient and reliable testing to ensure data security;

To summarize in a few words, Vault is a reliable, stable, simple, scalable, and low-cost storage system. Vault is a temporary name. If you have a better name, please let me know.