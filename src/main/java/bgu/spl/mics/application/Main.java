package bgu.spl.mics.application;
import bgu.spl.mics.application.passiveObjects.Attack;
import bgu.spl.mics.application.passiveObjects.Diary;
import bgu.spl.mics.application.passiveObjects.Ewoks;
import bgu.spl.mics.application.passiveObjects.Input;
import bgu.spl.mics.application.services.*;
import com.google.gson.*;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


/** This is the Main class of the application. You should parse the input file,
 * create the different components of the application, and run the system.
 * In the end, you should output a JSON.
 */
public class Main {
	public static void main(String[] args) {
		// write json
		//construct passive objects and microservices
		Input input = new Input();
		try {
			readInit(args[0], input);
		}
		catch (IOException e) { }
		Ewoks ewoks= Ewoks.getInstance(input.getEwoks());

		LandoMicroservice lando1 = new LandoMicroservice(input.getLando());
		R2D2Microservice R2D21 = new R2D2Microservice(input.getR2D2());
		LeiaMicroservice leia1 = new LeiaMicroservice(input.getAttacks());
		HanSoloMicroservice hanSolo1 = new HanSoloMicroservice();
		C3POMicroservice C3PO1 = new C3POMicroservice();

		List<Thread> list = new ArrayList<Thread>();
		list.add(new Thread(leia1));
		list.add(new Thread(lando1));
		list.add(new Thread(R2D21));
		list.add(new Thread(C3PO1));
		list.add(new Thread(hanSolo1));

		for (Thread t : list){
			t.start();
		}
		try{
		for (Thread t : list){
			t.join();
		}}
		catch (Exception e){}

		ewoks.resetEwoks();

		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		Diary finalD = Diary.getInstance();
			try {
				FileWriter fileWriter = new FileWriter(args[1]);
				gson.toJson(finalD, fileWriter);
				fileWriter.flush();
				fileWriter.close();
			} catch (Exception fileWriteException) {}





	}



	public static void readInit(String path,Input input) throws IOException {
		File jsonFile = Paths.get(path).toFile();
		Gson gson = new Gson();
		int R2D2, Lando, Ewoks;
		try (Reader reader = new FileReader(path)) {
			JsonObject jsonObject = gson.fromJson(new FileReader(jsonFile), JsonObject.class);
			JsonArray attacks = jsonObject.get("attacks").getAsJsonArray();
			Attack[] attackArr = new Attack[attacks.size()];
			int attackIndex = 0;
			for(JsonElement attackDes: attacks){
				JsonObject attack = attackDes.getAsJsonObject();
				int duration = attack.get("duration").getAsInt();
				JsonArray serials = attack.get("serials").getAsJsonArray();
				List<Integer> serialsList = new LinkedList<>();
				for(JsonElement num: serials){
					serialsList.add(num.getAsInt());
				}
				attackArr[attackIndex] = new Attack(serialsList, duration);
				attackIndex ++;
			}
			input.setAttacks(attackArr);
			input.setR2D2(jsonObject.get("R2D2").getAsLong());
			input.setLando(jsonObject.get("Lando").getAsLong());
			input.setEwoks(jsonObject.get("Ewoks").getAsInt());
		}
	}
}
