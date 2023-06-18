import 'package:flutter/material.dart';

void main() {
  runApp(const MyApp());
}

class Data {
  final int id;
  final String submitter;
  final String title;
  final String doi;

  Data(this.id, this.submitter, this.title, this.doi);
}

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  // This widget is the root of your application.
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      title: 'BD2 Project',
      theme: ThemeData(
        primarySwatch: Colors.blue,
      ),
      home: const MyHomePage(title: 'BD2 Project 2'),
    );
  }
}

class MyHomePage extends StatefulWidget {
  const MyHomePage({super.key, required this.title});

  final String title;

  @override
  State<MyHomePage> createState() => _MyHomePageState();
}

class _MyHomePageState extends State<MyHomePage> {
  final queryController = TextEditingController();
  final kController = TextEditingController();
  List<Data>? pythonData;
  List<Data>? postgresqlData;
  double? pythonTime;
  double? postgresqlTime;

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text(widget.title),
      ),
      body: SingleChildScrollView(
        child: Container(
          padding: const EdgeInsets.only(top: 20, left: 20, right: 20),
          width: MediaQuery.of(context).size.width,
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            children: [
              const Text('Enter your query here:'),
              const SizedBox(height: 20),
              TextFormField(
                minLines: 3,
                maxLines: 5,
                controller: queryController,
                decoration: const InputDecoration(
                  hintText: 'Enter your query here',
                  border: OutlineInputBorder(),
                ),
              ),
              const SizedBox(height: 20),
              const Text('Enter the value of K:'),
              const SizedBox(height: 20),
              TextFormField(
                controller: kController,
                decoration: const InputDecoration(
                  hintText: 'Top K',
                  border: OutlineInputBorder(),
                ),
              ),
              const SizedBox(height: 20),
              ElevatedButton(
                onPressed: () {
                  try {
                    if (queryController.text.isEmpty ||
                        kController.text.isEmpty) {
                      throw Exception('Empty query or K value');
                    }
                    final query = queryController.text;
                    final k = int.parse(kController.text);
                    print('Query: $query');
                    print('K: $k');
                  } catch (e) {
                    showDialog(
                        context: context,
                        builder: (context) {
                          return AlertDialog(
                            title: const Text('Error'),
                            content: const Text(
                                'Please enter a valid query and K value'),
                            actions: [
                              TextButton(
                                onPressed: () {
                                  Navigator.of(context).pop();
                                },
                                child: const Text('OK'),
                              )
                            ],
                          );
                        });
                  }
                },
                child: const Text('Submit'),
              ),
              const SizedBox(height: 40),
              Row(
                mainAxisAlignment: MainAxisAlignment.center,
                children: [
                  Expanded(
                      child: Column(
                    children: [
                      const Text('Top K - Python'),
                      const SizedBox(height: 50),
                      Table(
                        border: TableBorder.all(),
                        children: const [
                          TableRow(
                            children: [
                              Center(child: Text('id')),
                              Center(child: Text('submitter')),
                              Center(child: Text('title')),
                              Center(child: Text('doi')),
                            ],
                          ),
                        ],
                      ),
                      pythonData != null
                          ? Table(
                              border: TableBorder.all(),
                              children: pythonData!.map((data) {
                                return TableRow(
                                  children: [
                                    Center(child: Text(data.id.toString())),
                                    Center(child: Text(data.submitter)),
                                    Center(child: Text(data.title)),
                                    Center(child: Text(data.doi)),
                                  ],
                                );
                              }).toList(),
                            )
                          : const SizedBox(),
                      const SizedBox(height: 20),
                      pythonTime != null
                          ? Text('Time: $pythonTime')
                          : const SizedBox(),
                    ],
                  )),
                  const SizedBox(width: 50),
                  Expanded(
                      child: Column(
                    children: [
                      const Text('Top K - PostgreSQL'),
                      const SizedBox(height: 50),
                      Table(
                        border: TableBorder.all(),
                        children: const [
                          TableRow(
                            children: [
                              Center(child: Text('id')),
                              Center(child: Text('submitter')),
                              Center(child: Text('title')),
                              Center(child: Text('doi')),
                            ],
                          )
                        ],
                      ),
                      postgresqlData != null
                          ? Table(
                              border: TableBorder.all(),
                              children: postgresqlData!.map((data) {
                                return TableRow(
                                  children: [
                                    Center(child: Text(data.id.toString())),
                                    Center(child: Text(data.submitter)),
                                    Center(child: Text(data.title)),
                                    Center(child: Text(data.doi)),
                                  ],
                                );
                              }).toList(),
                            )
                          : const SizedBox(),
                      const SizedBox(height: 20),
                      postgresqlTime != null
                          ? Text('Time: $postgresqlTime')
                          : const SizedBox(),
                    ],
                  )),
                ],
              ),
            ],
          ),
        ),
      ),
    );
  }
}
