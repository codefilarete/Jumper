package org.gama.jumper.impl;

import javax.xml.bind.DatatypeConverter;

import org.junit.Test;

/**
 * @author Guillaume Mary
 */
public class ClassSignatureTest {
	
	@Test
	public void testGiveSignature() {
		System.out.println(DatatypeConverter.printHexBinary(new ClassSignature().giveSignature(this.getClass())));
	}
	
}