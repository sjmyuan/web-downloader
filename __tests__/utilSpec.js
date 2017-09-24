import { expect } from 'chai';
import { generateRages } from '../src/util';

describe('generateRages', () => {
    it('should empty array when size is 0', () => {
      expect(generateRages(20,0)).to.deep.equal([]);
    });

    it('should two parts when length is 10 and size is 6', () => {
      expect(generateRages(10,6)).to.deep.equal([{number:1, range:'bytes=0-5'}, {number:2, range:'bytes=6-9'}]);
    });

    it('should one parts when length is 10 and size is 10', () => {
      expect(generateRages(10,10)).to.deep.equal([{number:1, range:'bytes=0-9'}]);
    });
});
