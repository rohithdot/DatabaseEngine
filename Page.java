package database;

import java.io.RandomAccessFile;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Page{
	public static int pageSize = 512;
	public static final String datePattern = "yyyy-MM-dd_HH:mm:ss";


	
	public static int interiorpage(RandomAccessFile file)
	{
		int count = 0;
		try{
			count = (int)(file.length()/(new Long(pageSize)));
			count = count + 1;
			file.setLength(pageSize * count);
			file.seek((count-1)*pageSize);
			file.writeByte(0x05);  
		}catch(Exception e){
			System.out.println("Error at makeInteriorPage");
		}

		return count;
	}

	
	public static int leafpage(RandomAccessFile file)
	{
		int count = 0;
		try{
			count = (int)(file.length()/(new Long(pageSize)));
			count = count + 1;
			file.setLength(pageSize * count);
			file.seek((count-1)*pageSize);
			file.writeByte(0x0D);  
		}
		catch(Exception e)
		{
			System.out.println("Error at makeLeafPage");
		}

		return count;

	}

	
	public static int findmiddlekey(RandomAccessFile file, int page)
	{
		int val = 0;
		try{
			file.seek((page-1)*pageSize);
			byte type = file.readByte();
			
			int numCells = getCellNum(file, page);
			
			int m = (int) Math.ceil((double) numCells / 2);
			long location = getCellLoc(file, page, m-1);
			file.seek(location);

			switch(type){
				case 0x05:
					val = file.readInt(); 
					val = file.readInt();
					break;
				case 0x0D:
					val = file.readShort();
					val = file.readInt();
					break;
			}

		}catch(Exception e)
		{
			System.out.println("Error at findMidKey");
		}

		return val;
	}

	
	public static void splitLpage(RandomAccessFile file, int curPage, int newPage){
		try{
			
			int numCells = getCellNum(file, curPage);
			
			int mid = (int) Math.ceil((double) numCells / 2);

			int num1 = mid - 1;
			int num2 = numCells - num1;
			int content = 512;

			for(int i = num1; i < numCells; i++){
				long location = getCellLoc(file, curPage, i);
				
				file.seek(location);
				int cellSize = file.readShort()+6;
				content = content - cellSize;
				
				file.seek(location);
				byte[] cell = new byte[cellSize];
				file.read(cell);
				
				file.seek((newPage-1)*pageSize+content);
				file.write(cell);
				
				setCellOffset(file, newPage, i - num1, content);
			}

			
			file.seek((newPage-1)*pageSize+2);
			file.writeShort(content);

			
			short offset = getCellOffset(file, curPage, num1-1);
			file.seek((curPage-1)*pageSize+2);
			file.writeShort(offset);

			
			int rightMost = getRt(file, curPage);
			setRt(file, newPage, rightMost);
			setRt(file, curPage, newPage);

			
			int parent = getPar(file, curPage);
			setPar(file, newPage, parent);

			
			byte num = (byte) num1;
			setCellNum(file, curPage, num);
			num = (byte) num2;
			setCellNum(file, newPage, num);
		}catch(Exception e){
			System.out.println("Error at splitLeafPage");
			e.printStackTrace();
		}
	}
	
	public static void splitIpage(RandomAccessFile file, int curPage, int newPage)
	{
		try{
			
			int numCells = getCellNum(file, curPage);
			
			int mid = (int) Math.ceil((double) numCells / 2);

			int numCellA = mid - 1;
			int numCellB = numCells - numCellA - 1;
			short content = 512;

			for(int i = numCellA+1; i < numCells; i++){
				long loc = getCellLoc(file, curPage, i);
				
				short cellSize = 8;
				content = (short)(content - cellSize);
				
				file.seek(loc);
				byte[] cell = new byte[cellSize];
				file.read(cell);
			
				file.seek((newPage-1)*pageSize+content);
				file.write(cell);
			
				file.seek(loc);
				int page = file.readInt();
				setPar(file, page, newPage);
				
				setCellOffset(file, newPage, i - (numCellA + 1), content);
			}
			
			int tmp = getRt(file, curPage);
			setRt(file, newPage, tmp);
			
			long midLoc = getCellLoc(file, curPage, mid - 1);
			file.seek(midLoc);
			tmp = file.readInt();
			setRt(file, curPage, tmp);
			
			file.seek((newPage-1)*pageSize+2);
			file.writeShort(content);
			
			short offset = getCellOffset(file, curPage, numCellA-1);
			file.seek((curPage-1)*pageSize+2);
			file.writeShort(offset);

			
			int parent = getPar(file, curPage);
			setPar(file, newPage, parent);
			
			byte num = (byte) numCellA;
			setCellNum(file, curPage, num);
			num = (byte) numCellB;
			setCellNum(file, newPage, num);
		}catch(Exception e){
			System.out.println("Error at splitLeafPage");
		}
	}

	
	public static void splitlf(RandomAccessFile file, int page){
		int newPage = leafpage(file);
		int midKey = findmiddlekey(file, page);
		splitLpage(file, page, newPage);
		int parent = getPar(file, page);
		if(parent == 0)
		{
			int rootPage = interiorpage(file);
			setPar(file, page, rootPage);
			setPar(file, newPage, rootPage);
			setRt(file, rootPage, newPage);
			insertIntCell(file, rootPage, page, midKey);
		}
		else
		{
			long ploc = getPtrLoc(file, page, parent);
			setPtrLoc(file, ploc, parent, newPage);
			insertIntCell(file, parent, page, midKey);
			sortCell(file, parent);
			while(checkInteriorSpace(file, parent)){
				parent = splitint(file, parent);
			}
		}
	}

	
	public static int splitint(RandomAccessFile file, int page){
		int newPage = interiorpage(file);
		int midKey = findmiddlekey(file, page);
		splitIpage(file, page, newPage);
		int parent = getPar(file, page);
		if(parent == 0){
			int rootPage = interiorpage(file);
			setPar(file, page, rootPage);
			setPar(file, newPage, rootPage);
			setRt(file, rootPage, newPage);
			insertIntCell(file, rootPage, page, midKey);
			return rootPage;
		}else{
			long ploc = getPtrLoc(file, page, parent);
			setPtrLoc(file, ploc, parent, newPage);
			insertIntCell(file, parent, page, midKey);
			sortCell(file, parent);
			return parent;
		}
	}

	public static void sortCell(RandomAccessFile file, int page)
	{
		 byte num = getCellNum(file, page);
		 int[] keyArray = getKey(file, page);
		 short[] cellArray = getCellArr(file, page);
		 int ltmp;
		 short rtmp;

		 for (int i = 1; i < num; i++) 
		 {
            for(int j = i ; j > 0 ; j--)
            {
                if(keyArray[j] < keyArray[j-1])
                {

                    ltmp = keyArray[j];
                    keyArray[j] = keyArray[j-1];
                    keyArray[j-1] = ltmp;

                    rtmp = cellArray[j];
                    cellArray[j] = cellArray[j-1];
                    cellArray[j-1] = rtmp;
                }
            }
         }

         try
         {
         	file.seek((page-1)*pageSize+12);
         	for(int i = 0; i < num; i++)
         	{
				file.writeShort(cellArray[i]);
			}
         }catch(Exception e)
         {
         	System.out.println("Error at sortCellArray");
         }
	}

	public static int[] getKey(RandomAccessFile file, int page){
		int num = new Integer(getCellNum(file, page));
		int[] array = new int[num];

		try{
			file.seek((page-1)*pageSize);
			byte pageType = file.readByte();
			byte offset = 0;
			switch(pageType){
				case 0x05:
					offset = 4;
					break;
				case 0x0d:
					offset = 2;
					break;
				default:
					offset = 2;
					break;
			}

			for(int i = 0; i < num; i++){
				long loc = getCellLoc(file, page, i);
				file.seek(loc+offset);
				array[i] = file.readInt();
			}

		}catch(Exception e){
			System.out.println("Error at getKeyArray");
		}

		return array;
	}

	public static short[] getCellArr(RandomAccessFile file, int page){
		int num = new Integer(getCellNum(file, page));
		short[] array = new short[num];

		try{
			file.seek((page-1)*pageSize+12);
			for(int i = 0; i < num; i++){
				array[i] = file.readShort();
			}
		}catch(Exception e){
			System.out.println("Error at getCellArray");
		}

		return array;
	}

	
	public static int getPar(RandomAccessFile file, int page){
		int val = 0;

		try{
			file.seek((page-1)*pageSize+8);
			val = file.readInt();
		}catch(Exception e){
			System.out.println("Error at getParent");
		}

		return val;
	}

	public static void setPar(RandomAccessFile file, int page, int parent)
	{
		try{
			file.seek((page-1)*pageSize+8);
			file.writeInt(parent);
		}catch(Exception e){
			System.out.println("Error at setParent");
		}
	}

	
	public static long getPtrLoc(RandomAccessFile file, int page, int parent){
		long val = 0;
		try{
			int numCells = new Integer(getCellNum(file, parent));
			for(int i=0; i < numCells; i++){
				long loc = getCellLoc(file, parent, i);
				file.seek(loc);
				int childPage = file.readInt();
				if(childPage == page){
					val = loc;
				}
			}
		}catch(Exception e){
			System.out.println("Error at getPointerLoc");
		}

		return val;
	}

	
	public static void setPtrLoc(RandomAccessFile file, long loc, int parent, int page){
		try{
			if(loc == 0){
				file.seek((parent-1)*pageSize+4);
			}else{
				file.seek(loc);
			}
			file.writeInt(page);
		}catch(Exception e){
			System.out.println("Error at setPointerLoc");
		}
	} 

	public static void insertIntCell(RandomAccessFile file, int page, int child, int key)
	{
		try{
			
			file.seek((page-1)*pageSize+2);
			short content = file.readShort();
			if(content == 0)
				content = 512;
			content = (short)(content - 8);
			
			file.seek((page-1)*pageSize+content);
			file.writeInt(child);
			file.writeInt(key);
			
			file.seek((page-1)*pageSize+2);
			file.writeShort(content);
			
			byte num = getCellNum(file, page);
			setCellOffset(file, page ,num, content);
			
			num = (byte) (num + 1);
			setCellNum(file, page, num);

		}catch(Exception e){
			System.out.println("Error at insertInteriorCell");
		}
	}

	
	public static void insertLfCell(RandomAccessFile file, int page, int offset, short plsize, int key, byte[] stc, String[] vals, String table){
		try{
			String s;
			file.seek((page-1)*pageSize+offset);
			String[] colName = CatalogReader.getColName(table);
			
			
			
			file.seek((page-1)*pageSize+offset);
			file.writeShort(plsize);
			file.writeInt(key);
			int col = vals.length - 1;
			
			
			file.writeByte(col);
			file.write(stc);
			
			for(int i = 1; i < vals.length; i++)
				
			{	
				switch(stc[i-1]){
					case 0x00:
						file.writeByte(0);
						break;
					case 0x01:
						file.writeShort(0);
						break;
					case 0x02:
						file.writeInt(0);
						break;
					case 0x03:
						file.writeLong(0);
						break;
					case 0x04:
						file.writeByte(new Byte(vals[i]));
						break;
					case 0x05:
						file.writeShort(new Short(vals[i]));
						break;
					case 0x06:
						file.writeInt(new Integer(vals[i]));
						break;
					case 0x07:
						file.writeLong(new Long(vals[i]));
						break;
					case 0x08:
						file.writeFloat(new Float(vals[i]));
						break;
					case 0x09:
						file.writeDouble(new Double(vals[i]));
						break;
					case 0x0A:
						s = vals[i];
						
						Date temp = new SimpleDateFormat(datePattern).parse(s);
						long time = temp.getTime();
						file.writeLong(time);
						break;
					case 0x0B:
						s = vals[i];
						
						s = s+"_00:00:00";
						Date temp2 = new SimpleDateFormat(datePattern).parse(s);
						long time2 = temp2.getTime();
						file.writeLong(time2);
						break;
					default:
						file.writeBytes(vals[i]);
						break;
				}
			}
			int n = getCellNum(file, page);
			byte tmp = (byte) (n+1);
			setCellNum(file, page, tmp);
			file.seek((page-1)*pageSize+12+n*2);
			file.writeShort(offset);
			file.seek((page-1)*pageSize+2);
			int content = file.readShort();
			if(content >= offset || content == 0){
				file.seek((page-1)*pageSize+2);
				file.writeShort(offset);
			}
		}catch(Exception e)
		{
			System.out.println("Error at insertLeafCell");
			e.printStackTrace();
		}
	}

	public static void upLfCell(RandomAccessFile file, int page, int offset, int plsize, int key, byte[] stc, String[] vals, String table){
		try{
			String s;
			file.seek((page-1)*pageSize+offset);
			file.writeShort(plsize);
			file.writeInt(key);
			int col = vals.length - 1;
			file.writeByte(col);
			file.write(stc);
			for(int i = 1; i < vals.length; i++){
				
				switch(stc[i-1]){
					case 0x00:
						file.writeByte(0);
						break;
					case 0x01:
						file.writeShort(0);
						break;
					case 0x02:
						file.writeInt(0);
						break;
					case 0x03:
						file.writeLong(0);
						break;
					case 0x04:
						file.writeByte(new Byte(vals[i]));
						break;
					case 0x05:
						file.writeShort(new Short(vals[i]));
						break;
					case 0x06:
						file.writeInt(new Integer(vals[i]));
						break;
					case 0x07:
						file.writeLong(new Long(vals[i]));
						break;
					case 0x08:
						file.writeFloat(new Float(vals[i]));
						break;
					case 0x09:
						file.writeDouble(new Double(vals[i]));
						break;
					case 0x0A:
						s = vals[i];
						Date temp = new SimpleDateFormat(datePattern).parse(s.substring(1, s.length()-1));
						long time = temp.getTime();
						file.writeLong(time);
						break;
					case 0x0B:
						s = vals[i];
						s = s.substring(1, s.length()-1);
						s = s+"_00:00:00";
						Date temp2 = new SimpleDateFormat(datePattern).parse(s);
						long time2 = temp2.getTime();
						file.writeLong(time2);
						break;
					default:
						file.writeBytes(vals[i]);
						break;
				}
			}
		}catch(Exception e){
			System.out.println("Error at Page.update");
			System.out.println(e);
		}
	}

	public static int getRt(RandomAccessFile file, int page)
	{
		int val = 0;

		try
		{
			file.seek((page-1)*pageSize+4);
			val = file.readInt();
		}
		catch(Exception e)
		{
			System.out.println("Error in rightmost");
		}

		return val;
	}

	public static void setRt(RandomAccessFile file, int page, int rightMost)
	{
		try
		{
			file.seek((page-1)*pageSize+4);
			file.writeInt(rightMost);
		}
		catch(Exception e)
		{
			System.out.println("Error in setting rightmost");
		}

	}

	
	public static byte getCellNum(RandomAccessFile file, int page)
	{
		byte val = 0;
		try
		{
			file.seek((page-1)*pageSize+1);
			val = file.readByte();
		}
		catch(Exception e)
		{
			System.out.println(e);
			System.out.println("Error at getCellNumber");
		}
		return val;
	}

	public static void setCellNum(RandomAccessFile file, int page, byte num)
	{
		try{
			file.seek((page-1)*pageSize+1);
			file.writeByte(num);
		}catch(Exception e){
			System.out.println("Error at setCellNumber");
		}
	}

	public static boolean checkInteriorSpace(RandomAccessFile file, int page)
	{
		byte numCells = getCellNum(file, page);
		if(numCells > 30)
			return true;
		else
			return false;
	}

	
	public static int checkLeafSpace(RandomAccessFile file, int page, int size)
	{
		int val = -1;

		try
		{
			file.seek((page-1)*pageSize+2);
			int content = file.readShort();
			if(content == 0)
				return pageSize - size;
			int numCells = getCellNum(file, page);
			int space = content - 20 - 2*numCells;
			if(size < space)
				return content - size;
			
		}catch(Exception e)
		{
			System.out.println("Error at checkLeafSpace");
		}

		return val;
	}

	
	public static boolean hasKey(RandomAccessFile file, int page, int key)
	{
		int[] array = getKey(file, page);
		for(int i : array)
			if(key == i)
				return true;
		return false;
	}

	
	public static long getCellLoc(RandomAccessFile file, int page, int id)
	{
		long loc = 0;
		try{
			file.seek((page-1)*pageSize+12+id*2);
			short offset = file.readShort();
			long orig = (page-1)*pageSize;
			loc = orig + offset;
		}
		catch(Exception e)
		{
			System.out.println("Error at getCellLoc");
		}
		return loc;
	}

	public static short getCellOffset(RandomAccessFile file, int page, int id)
	{
		short offset = 0;
		try{
			file.seek((page-1)*pageSize+12+id*2);
			offset = file.readShort();
		}
		catch(Exception e)
		{
			System.out.println("Error at getCellOffset");
		}
		return offset;
	}

	public static void setCellOffset(RandomAccessFile file, int page, int id, int offset){
		try{
			file.seek((page-1)*pageSize+12+id*2);
			file.writeShort(offset);
		}
		catch(Exception e)
		{
			System.out.println("Error at setCellOffset");
		}
	}
}
