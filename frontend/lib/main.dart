import 'dart:convert';
import 'dart:io';

import 'package:flutter/material.dart';

void main() {
  runApp(const MyApp());
}

void showLoadingPopup(BuildContext context) {
  showDialog(
      context: context,
      builder: (context) {
        return AlertDialog(
          content: Row(
            children: const [
              CircularProgressIndicator(),
              SizedBox(width: 20),
              Text('Loading'),
            ],
          ),
        );
      });
}

Future<Map<String, dynamic>?> onSearch(
    BuildContext context, String query, int k) async {
  try {
    showLoadingPopup(context);
    const command =
        "/Users/mariojacoboriosgamboa/Lordmarcusvane/executables/miniforge3/bin/python3";
    final arguments = ['main.py', query, k.toString()];
    final process = await Process.run(command, arguments,
        workingDirectory:
            '/Users/mariojacoboriosgamboa/Lordmarcusvane/Hackprog/universidad/bd2/bd2_project_2/backend');
    if (process.exitCode == 0) {
      final output = process.stdout.toString();
      // parse with JSON
      final json = jsonDecode(output);
      Navigator.of(context).pop();
      return json;
    } else {
      Navigator.of(context).pop();
      showDialog(
          context: context,
          builder: (context) {
            return AlertDialog(
              title: const Text('Error'),
              content: const Text('An error has occurred'),
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
    return null;
  } catch (e) {
    Navigator.of(context).pop();
    return null;
  }
}

class Data {
  final String id;
  final String submitter;
  final String title;
  final String doi;
  final double score;

  const Data({
    required this.id,
    required this.submitter,
    required this.title,
    required this.doi,
    required this.score,
  });

  factory Data.fromJson(Map<String, dynamic> json) {
    return Data(
      id: json['doc_id'],
      submitter: json['submitter'],
      title: json['title'],
      doi: json['doi'],
      score: json['score'],
    );
  }
}

class MyApp extends StatelessWidget {
  const MyApp({super.key});

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
          padding: const EdgeInsets.all(20),
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
                onPressed: () async {
                  try {
                    if (queryController.text.isEmpty ||
                        kController.text.isEmpty) {
                      throw Exception('Empty query or K value');
                    }
                    final query = queryController.text;
                    final k = int.parse(kController.text);
                    final result = await onSearch(context, query, k);
                    if (result == null) {
                      return;
                    }
                    final pythonResult =
                        result['python'] as Map<String, dynamic>;
                    final postgresqlResult =
                        result['postgreSQL'] as Map<String, dynamic>;

                    pythonTime = pythonResult['time'] as double;
                    postgresqlTime = postgresqlResult['time'] as double;


                    final pythonResultList =
                        pythonResult['result'] as List<dynamic>;
                    final postgresqlResultList =
                        postgresqlResult['result'] as List<dynamic>;

                    pythonData = pythonResultList
                        .map((e) => Data.fromJson(e as Map<String, dynamic>))
                        .toList();

                    postgresqlData = postgresqlResultList
                        .map((e) => Data.fromJson(e as Map<String, dynamic>))
                        .toList();

                    setState(() {});
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
                crossAxisAlignment: CrossAxisAlignment.start,
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
                              Center(child: Text('score')),
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
                                    Center(child: Text(data.score.toString())),
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
                          ? Text('Time: $pythonTime s')
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
                              Center(child: Text('score')),
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
                                    Center(child: Text(data.score.toString())),
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
                          ? Text('Time: $postgresqlTime s')
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
